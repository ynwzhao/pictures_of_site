# -*- coding:utf-8 -*-
"""
python spider.py https://www.dbmeinv.com/
"""
import os
import sys
import json
import urllib2
import redis
import urlparse
import requests
import traceback
from copy import deepcopy
from lxml import etree


HEADERS = {
    'User-Agent': 'Baiduspider-image',
}


class Queue:
    def __init__(self):
        self.list_name = 'task'
        self._redis_conn = redis.Redis()

    def put(self, task):
        self._redis_conn.lpush(self.list_name, task)

    def get(self):
        return self._redis_conn.brpop(self.list_name, timeout=60)


class DedupMap:
    def __init__(self):
        self.set_name = 'visited'
        self._redis_conn = redis.Redis()

    def first_visit(self, element):
        return self._redis_conn.sadd(self.set_name, element)

    def retry(self, element):
        self._redis_conn.srem(self.set_name, element)


TASK_QUEUE = Queue()  # 网页任务调度，redis队列
DEDUP_MAP = DedupMap()  # 网页去重, redis集合
FILTER = {
    'href': lambda x: True,
    'src': lambda x: True,
}


def dn_of_url(url):
    """获取url的服务器域名"""
    return urlparse.urlparse(url).netloc


def ensure_dir(path):
    """确保目录存在"""
    if not os.path.exists(path):
        os.mkdir(path)


def full_path(href, refer):
    parse = urlparse.urlparse(refer)
    if href.startswith('http://') or href.startswith('https://'):
        rtv = href
    elif href.startswith('/'):
        rtv = '%s://%s%s' % (parse.scheme, parse.netloc, href)
    elif href.startswith('#'):
        query = '?' + parse.query if parse.query else ''
        rtv = '%s://%s%s%s%s' % (parse.scheme, parse.netloc, parse.path, query, href)
    elif href.startswith('?'):
        rtv = '%s://%s%s%s' % (parse.scheme, parse.netloc, parse.path, href)
    elif href.startswith('javascript'):
        rtv = refer
    else:
        rtv = '%s://%s%s' % (parse.scheme, parse.netloc, os.path.join(os.path.dirname(parse.path), href))
    return rtv


def extract_src_list(text):
    if not text:
        return []
    tree = etree.HTML(text)
    return tree.xpath('//img/@src')


def extract_href_list(text):
    if not text:
        return []
    tree = etree.HTML(text)
    return tree.xpath('//a/@href')


def get_html_content(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text


def get_image_content(url, headers):
    request = urllib2.Request(url=url, headers=headers)
    socket = urllib2.urlopen(request)
    if socket.code == 200:
        return socket.read()


def get_next_urls(url, html):
    """
    html是HTTP请求url获得的内容
    """
    src_list = [full_path(src, url) for src in extract_src_list(html)]
    href_list = [full_path(href, url) for href in extract_href_list(html)]
    return src_list, href_list


def download_img(url, headers):
    path = os.path.join(os.path.dirname(__file__), os.path.basename(os.path.dirname(url)))
    ensure_dir(path)
    file_name = os.path.join(path, os.path.basename(url))
    if os.path.exists(file_name):
        return False

    content = get_image_content(url, headers)
    if content:
        with open(file_name, 'wb') as fp:
            fp.write(content)
        return True


def deep_crawl(url, headers, page_type):
    print page_type, url
    if page_type == 'src':  # 图片下载，失败延后重试
        try:
            succeed = download_img(url, headers)
            if succeed:
                print 'OK down: ', url
        except BaseException as e:
            print 'ERROR down: ', e
            raise e

    elif page_type == 'href':
        headers = deepcopy(HEADERS)
        headers['Referer'] = url

        html = get_html_content(url, headers)
        src_list, href_list = get_next_urls(url, html)

        for src in src_list:
            if FILTER['src'](src):
                TASK_QUEUE.put(json.dumps({'url': src, 'headers': headers, 'type': 'src'}))

        for href in href_list:
            if FILTER['href'](href) and DEDUP_MAP.first_visit(href):
                TASK_QUEUE.put(json.dumps({'url': href, 'headers': headers, 'type': 'href'}))


def main(index_url):
    # 定制图片抓取规则和进一步爬取规则
    FILTER['href'] = lambda x: dn_of_url(index_url) in x
    # FILTER['src'] = lambda x: dn_of_url(index_url) in x

    headers = deepcopy(HEADERS)
    TASK_QUEUE.put(json.dumps({'url': index_url, 'headers': headers, 'type': 'href'}))
    while True:
        task_data = TASK_QUEUE.get()
        if not task_data:
            break
        else:
            _queue_name, task = task_data
            task = json.loads(task)
            try:
                deep_crawl(task['url'], task['headers'], task['type'])
            except BaseException as e:  # 失败的时候把它重新放回去
                print 'PUT BACK:', task
                TASK_QUEUE.put(json.dumps(task))
                print traceback.format_exc()
                raise e


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print sys.argv[0], 'url'
    else:
        main(sys.argv[1])
