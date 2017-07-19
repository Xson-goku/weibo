# coding: utf-8

__version__ = '1.1.0'
__author__ = 'chenmin (1019717007@qq.com)'

'''
以关键词收集新浪微博
'''
import urllib
from bs4 import BeautifulSoup
import urllib2
import re
import json
import hashlib
import os
import time
import datetime
import random
import logging
import sys
import redis_db
from headers import headers
import pymongo
import requests
import common_utils

reload(sys)
sys.setdefaultencoding('utf8')

class CollectData():
    """单条微博及用户信息收集类

        大体思路：利用微博API获取单条微博内容、评论数、点赞数、转发数、第一页评论、作者信息等内容

        单条微博内容获取地址：https://m.weibo.cn/status/4110221791827771

        第一页评论数据获取地址：https://m.weibo.cn/api/comments/show?id=4078996901408644&page=1
    """
    def __init__(self, brand_id,mid,collection,crawl_date,weibo_url = "https://m.weibo.cn/status/",comments_url="https://m.weibo.cn/api/comments/show?id="):
        self.weibo_url = weibo_url  #设置固定地址部分，默认为"https://m.weibo.cn/status/"
        self.mid = mid
        self.collection = collection
        self.comments_url = comments_url
        self.crawl_date= crawl_date #设置爬虫日期
        self.setBrandId(brand_id)   #设置品牌id
        self.logger = logging.getLogger('main.CollectData') #初始化日志

    ##设置品牌id
    def setBrandId(self, brand_id):
        self.brand_id = brand_id

    ##构建微博URL
    def getWeiboURL(self):
        return self.weibo_url+self.mid
    ##构建第一页评论URL
    def getCommentsURL(self):
        return self.comments_url+self.mid+"&page=1"
    ##爬取具体数据
    def download(self, maxTryNum=4):
        proxies = common_utils.get_proxies()
        weibo_url = self.getWeiboURL()
        comments_url = self.getCommentsURL()
        print '-----weibo_url: '+weibo_url
        for tryNum in range(maxTryNum):
            try:
                resp = requests.get(weibo_url, headers=headers,proxies = proxies,timeout=3, verify=False)
                comment_resp = requests.get(comments_url, headers=headers,proxies = proxies,timeout=3, verify=False)
            except:
                 if tryNum < (maxTryNum-1):
                    time.sleep(0.1)
                    proxies = common_utils.get_proxies()
                 else:
                    print 'Internet Connect Error!'
                    redis_db.Mids.store_mid(self.brand_id,self.mid,self.crawl_date)
                    ###########增加短信验证码
                    ###hostname = common_utils.get_hostname()
                    ###message_content = 'url:'+str(weibo_url)
                    ###common_utils.send_messag("18561906132",message_content)
                    ##print "I'll sleep 30s..."
                    ###time.sleep(30)
                    return
        try:
            response_data = resp.text
            comment_data = comment_resp.json()
            response_str = response_data.replace('\n','')
            ###微博返回数据
            render_data = re.findall(".*render_data = (.*);</script>",response_str)
            render_json = json.loads(render_data[0][1:-10])
        except:
           print '---------------------------------weibo has been deleted'
           redis_db.Mids.store_mid(self.brand_id,self.mid,self.crawl_date)
           return
        weibo_dict={}
        weibo_dict['mid']=self.mid
        weibo_dict['brand_id']=self.brand_id
        weibo_dict['crawl_date']=self.crawl_date
        weibo_dict['render_data']=render_json
        weibo_dict['comment_data']=comment_data
        self.collection.insert(weibo_dict)

#####准备爬虫
def crawlData(set_name,collection):
    mid_keys = redis_db.Mids.fetch_mid(set_name)
    if mid_keys is not None:
       crawlDate = str(datetime.date.today()) 
       brand_id = mid_keys[0]
       mid = mid_keys[1]
       ##实例化收集类，收集指定关键字和起始时间的微博
       cd = CollectData(brand_id,mid,collection,crawlDate)
       cd.download()
def main():
    logger = logging.getLogger('main')
    logFile = './run_collect.log'
    logger.setLevel(logging.DEBUG)
    filehandler = logging.FileHandler(logFile)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)
    client = pymongo.MongoClient("139.129.222.132", 27017)
    db=client['weibo_db']
    collection=db['weibo']
    while True:
        #####进行今天数据搜索
        crawlData("weibo:mids",collection)
    else:
        logger.removeHandler(filehandler)
        logger = None
if __name__ == '__main__':
    main()
