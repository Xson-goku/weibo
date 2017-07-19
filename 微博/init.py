# -*- coding: utf-8 -*-
import login
import pymysql
import time
import json
import requests
import redis_db
import datetime

def getCrawlDate(num): 
    today=datetime.date.today() 
    delta=datetime.timedelta(days=num) 
    crawlDate=today-delta  
    return str(crawlDate)
def storeKeywords():
  ##获取需要抓取的关键词
  conn = pymysql.connect('localhost','own', 'Gx2!@dssfde$$$11', 'weibo_db', charset="utf8")
  cur = conn.cursor()
 
  sql = "select id,name,nick_name from brand"
  cur.execute(sql)
  rows = cur.fetchall()
  for row in rows:
    brand_id = row[0]
    brand_name = row[1]
    nick_name = row[2]
    redis_db.Keywords.store_keywords(brand_id,brand_name.encode("utf-8"))
    if nick_name:
        nick_name_list = nick_name.split(',')
        for nick_item in nick_name_list:
            redis_db.Keywords.store_keywords(brand_id,nick_item.encode("utf-8"))
  cur.close()
  conn.close()
  print '需要抓取的关键词已获取完毕'

def main(self):
   
  print "start crawler at:" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))
  
  #####将当日未搜索的所有关键词放入历史set中
  redis_db.Keywords.move_keywords()
  print '昨日未搜索的所有关键词已放入历史set中'
  #####将当日未搜索的所有微博mid放入历史set中
  redis_db.Mids.move_mids()
  print '昨日未搜索的weibo mids已放入历史set中'
  
  ##获取需要抓取的关键词
  storeKeywords()

  set1 = 'weibo:mids:'+getCrawlDate(2)
  set2 = 'weibo:mids:'+getCrawlDate(3)
  set3 = 'weibo:mids:'+getCrawlDate(4)
  set4 = 'weibo:mids:'+getCrawlDate(5)
  set5 = 'weibo:mids:'+getCrawlDate(6)
  set6 = 'weibo:mids:'+getCrawlDate(7)
  redis_db.Mids.mids_unionstore('weibo:mids','weibo:mids',set1,set2,set3,set4,set5,set6)
  print '执行登录操作'
  #login.store_account()
  print "crawler-init finished at:" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))
if __name__ == '__main__':
    main()
