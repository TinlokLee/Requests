'''
    项目改写：
        1 基于requests，爬虫项目
        2 多线程实现
        3 数据抓取和解析

'''

# -*- coding:utf-8 -*-

import requests
from lxml import etree
from Queue import Queue
import threading
import time
import json


class thead_crawl(threading.Thread):
    '''抓取线程'''
    def __init__(self, threadID, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.q = q 

    def run(self):
        print('Starting' + self.threadID)
        self.qiushi_spider()
        print('Exiting', self.threadID)

    def qishi_spider(self):
        while True:
            if self.q.empty():
                break
            else:
                page = self.q.get()
                print('qiushi_spider=', self.threadID, 'page=', str(page))
                url = 'http://www.qiushibaike.com/hot/page/' + str(
page) + '/'
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WO
W64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Sa
fari/537.36', 'Accept-Language': 'zh-CN,zh;q=0.8'}
                timeout = 3
                while timeout > 0:
                    timeout -= 1
                    try:
                        content = requests.get(url=url, headers=headers)
                        data_queue.put(content.text)
                        break
                    except Exception as e:
                        print(e)
                if timeout < 0:
                    print('timeout', url)


class Thread_Parser(threading.Thread):
    '''解析页面'''
    def __init__(self, threadID, queue, lock, f):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.queue = queue
        self.lock = lock
        self.f = f

    def run(self):
        print('Starting', self.threadID)
        global total, exitFlag_Parser
        while not exitFlag_Parser:
            try:
                item = self.queue.get(False)
                if not item:
                    pass
                self.parse_data(item)
                self.queue.task_done()
                print('Thread_Parser=', self.threadID, 'total=', total)
            except:
                pass
        print('Exiting', self.threadID)

    def parse_data(self, item):
        global total
        try:
            html = etree.HTML(item)
            result = html.xpath('//div[contains(@id,"qiushi_tag")]'
)
            for site in result:
                try:
                    imgUrl = site.xpath('.//img/@src')[0]
                    title = site.xpath('.//h2')[0].text
                    content = site.xpath('.//div[@class="content"]/
span')[0].text.strip()
                    vote = None
                    comments = None
                    try:
                        vote = site.xpath('.//i')[0].text
                        comments = site.xpath('.//i')[1].text
                    except:
                        pass
                    result = {
                        'imgUrl': imgUrl,
                        'title': title,
                        'content': content,
                        'vote': vote,
                        'comments': comments,
                    }
                    with self.lock:
                        self.f.write(json.dumps(result, ensure_asci
i=False).encode('utf-8') + "\n")
                except Exception as e:
                    print('parse_data', e)
                with self.lock:
                    total += 1
                
data_queue = Queue()
exitFlag_Parser = False
lock = threading.lock()
total = 0

def main():
    output = open('qiushibaike.json', 'a')

    # 初始化前5页
    pageQueue = Queue(50)
    for page in range(1, 6):
        pageQueue.put(page)

    # 初始化采集线程
    crawlthreads = []
    crawlList = ["crawl-1", "crawl-2", "crawl-3"]

    for threadID in crawlList:
        thread = thread_crawl(threadID, pageQueue)
        thread.start()
        crawlthreads.append(thread)

    # 初始化解析线程
    parserthreads = []
    parserList = ["parser-1", "parser-2", "parser-3"]
    for threadID in parserList:
        thread = Thread_Parser(threadID, data_queue, lock, output)
        thread.start()
        parserthreads.append(thread)
    
    # 等待队列清空
    while not pageQueue.empty():
        pass
    
    # 等待所有线程完成
    for t in crawlthreads:
        t.join()

    while not data_queue.empty():
        pass

    # 通知线程退出
    global exitFlag_Parser
    exitFlag_Parser = True
    for t in parserthreads:
        t.join()
    print('Exiting Main Thread')
    with lock:
        output.close()


if __name__ == "__main__":
    main()

    



    
    




