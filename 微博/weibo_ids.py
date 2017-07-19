# coding: utf-8

__version__ = '1.1.0'
__author__ = 'chenmin (1019717007@qq.com)'

'''
以关键词收集新浪微博
'''
import urllib
from bs4 import BeautifulSoup
import json
import time
import datetime
import random
import logging
import sys
import redis_db
from headers import headers
import requests
import common_utils

reload(sys)
sys.setdefaultencoding('utf8')

COOKIES_LIST = redis_db.Cookies.getCookiesList()
class CollectMids():
    """数据收集类
        利用微博高级搜索功能，按关键字搜集一定时间范围内的微博id。

        大体思路：构造URL，爬取网页，然后解析网页中的微博ID。本程序只负责收集微博的ID。

        登陆新浪微博，进入高级搜索，输入关键字”空气污染“，时间为”2017-07-04:2017-07-04“，之后发送请求会发现地址栏变为如下：
        http://s.weibo.com/weibo/%25E7%25A9%25BA%25E6%25B0%2594%25E6%25B1%25A1%25E6%259F%2593&typeall=1&suball=1&timescope=custom:2017-07-04:2017-07-04&Refer=g
        固定地址部分：http://s.weibo.com/weibo/
        关键字二次UTF-8编码：%25E7%25A9%25BA%25E6%25B0%2594%25E6%25B1%25A1%25E6%259F%2593
        搜索地区：region=custom:11:1000
        搜索时间范围：timescope=custom:2013-07-02-2:2013-07-09-2
        可忽略项：Refer=g
        显示类似微博：nodup=1    注：这个选项可多收集微博，建议加上。默认不加此参数，省略了部分相似微博。
        某次请求的页数：page=1
        搜索类型： typeall=1 全部
        包含（搜索到的内容，全部、含图片、含视频含短链接）：suball=1 全部
        为了降低微博帐号被封的几率，每台服务器在进行搜索前，从redis中获取5个cookie存入cookie_list中，每次关键词发生变化或者某次搜索的页数等于20时，
        从cookie_list中随机取出一个cookie，当检测到某个cookie失效后，将其从cookie_list中移除，并将账号登录名写入至redis的set:invalid:account中
        另外，高级搜索最多返回50页微博，针对此限制，出于以下两种考虑：1、尽量获取全部数据 2、尽可能的减少请求次数。选择了以下方案：
            首先设置时间间隔为一天，地域字段不设置，page=50，检测返回的微博条数是否不少于15条，
                如果不少于则认为微博未抓取完全，对时间间隔进行分割，按小时进行分割（凌晨2点至7点，是微博的低发布期，将其设为一个单独的时间段）
                    将时间间隔设置为小时，地域字段不设置，page=50，检测返回的微博条数是否不少于15条
                        如果不少于15条，则加上地域字段参数，对地区字段进行遍历获取微博
                        如果少于15条，则一页页的对微博进行抓取，直到page=49或者hasMore字段为false时结束此关键词的爬虫
                如果少于15条，则认为按天搜索可以搜索到所有微博，则一页页的对微博进行抓取，直到page=49或者hasMore字段为false时结束此关键词的爬虫
       
    """
    def __init__(self, keyword, interval, brand_id, begin_url_per = "http://s.weibo.com/weibo/"):
        self.begin_url_per = begin_url_per  #设置固定地址部分，默认为"http://s.weibo.com/weibo/"
        self.setKeyword(keyword)    #设置关键字
        self.interval = int(interval)  #设置邻近网页请求之间的基础时间间隔（注意：过于频繁会被认为是机器人）
        self.brand_id=brand_id   #设置品牌id
        self.logger = logging.getLogger('main.CollectData') #初始化日志

    ##设置关键字
    def setKeyword(self, keyword):
        self.keyword = keyword.encode("utf-8")

    ##构建URL
    def getURL(self,timescope):
        return self.begin_url_per+self.getKeyWord()+"&typeall=1&suball=1&nodup=1&timescope=custom:"+timescope

    ##关键字需要进行两次urlencode
    def getKeyWord(self):
        once = urllib.urlencode({"kw":self.keyword})[3:]
        return urllib.urlencode({"kw":once})[3:]
	
    ##爬取一次请求中的所有网页，最多返回50页
    def download(self, url,cookies,name,maxTryNum=4):
        print '-----url: '+url
        intterval = common_utils.get_sleep_time()
        time.sleep(intterval)
        ##网络不好的情况，试着尝试请求三次
        for tryNum in range(maxTryNum):
            try:
                resp = requests.get(url, headers=headers, cookies=cookies,timeout=3)
                resp_data = resp.text
                return resp_data
            except: 
                if tryNum < (maxTryNum-1):
                    print '---send request has been suffered exception,url:'+url
                    time.sleep(10)
                else:
                    print 'Internet Connect Error!'
                    ###########增加短信验证码
                    hostname = common_utils.get_hostname()
                    message_content = 'url:'+url+',account:'+name+',hostname:'+hostname
                    common_utils.send_messag("18561906132",message_content)
                    print "I'll sleep 30s..."
                    time.sleep(30)
                    return 'None'
    ####分析爬虫返回数据,并将微博id存入至redis
    def analysis_data(self,data,crawlDate):
        if data is 'None':
           print '-------data is none'
           isCaught = True
           hasMore = False
           mids_size = 0
           return isCaught,hasMore,mids_size
        #self.logger.info("---data:"+data)
        if data.find("noresult_tit")>=0:
           ###无搜索结果
           print '------------------------noresult'
           isCaught = False
           hasMore = False
           mids_size = 0
           return isCaught,hasMore,mids_size
        lines = data.splitlines()
        isCaught = True ##是否被抓住
        hasMore = True ##是否有下一页
        mids_size = 0 ##页面上包括的微博条数
        for line in lines:
            ## 判断是否有微博内容，出现这一行，则说明没有被认为是机器人
            if line.startswith('<script>STK && STK.pageletM && STK.pageletM.view({"pid":"pl_weibo_direct"'):
                isCaught = False
                n = line.find('html":"')
                if n > 0:
                    j = line[n + 7: -12].encode("utf-8").decode('unicode_escape').replace("\/","/")
                    ## 有结果的页面
                    if j.find('class="page next S_txt1 S_line1"')<1: 
                       hasMore = False
                    soup=BeautifulSoup(j, "html.parser") 
                    dls=soup.find_all('div',attrs={'mid':True})
                    mids_size = len(dls)
                    for dl in dls:
                        mid = dl['mid']
                        ###存储数据
                        redis_db.Mids.store_mid(self.brand_id,mid,crawlDate)
        return isCaught,hasMore,mids_size

####获取需要爬取的日期
def getCrawlDate(num): 
    today=datetime.date.today() 
    delta=datetime.timedelta(days=num) 
    crawlDate=today-delta  
    return str(crawlDate)

###每次从redis中获取五个cookie，当有cookie失效
def getCookiesList():
    i = 0;
    cookies_list =[]
    while i <5:
        cookie_dict = {}
        cookies,name = redis_db.Cookies.fetch_cookies()
        cookie_dict['cookie']=cookies
        cookie_dict['name']=name
        cookie_dict['err_times']=0
        cookies_list.append(cookie_dict)
    return cookie_list

####获取cookie
def getCookie():
    global COOKIES_LIST
    cookies_size = len(COOKIES_LIST)
    ###可用cookie数量是否小于3
    ###如果小于3则去redis中获取cookie，补足5个cookie
    if cookies_size < 3:
        i = 0
        while i<(5-cookies_size):
            cookie_dict = {}
            cookies,name = redis_db.Cookies.fetch_cookies()
            cookie_dict['cookie']=cookies
            cookie_dict['name']=name
            cookie_dict['err_times']=0
            COOKIES_LIST.append(cookie_dict)
            i = i + 1
        cookie_sizes = 5
    num = random.randint(0,cookies_size-1)
    return COOKIES_LIST[num],num

###爬虫异常处理函数,判断该cookie失败次数是否大于4次，
###如果大于4次则认为这个cookie已经失效，该账号需要人工解封（登录浏览器输入验证码或者发短信解封），将其从cookie_list中移除，并将失效账号存入redis中
###如果小于4次，则只将其err_times+1
def caughtAction(url,data,num,cookie_dict):
    print "Be Caught,url"+url
    logger.info('url:'+url)
    logger.info("---data:"+data)
    err_num = cookie_dict['err_times']
    if err_num >4:
        del COOKIES_LIST[num]
        print "!!!!!!!!account maybe invalid:"+cookie_dict['name']
        logger.info("!!!!!!!!account maybe invalid:"+cookie_dict['name'])
        redis_db.Cookies.store_invalid_name(cookie_dict['name'])
    else:
        cookie_dict['err_times'] = err_num + 1
        COOKIES_LIST[num] = cookie_dict

#####准备爬虫
def crawlData(set_name,crawlDate):
    global COOKIES_LIST
    logger = logging.getLogger('main.Collectmids') 
    brandId_keys = redis_db.Keywords.fetch_keyword(set_name)
    region_list = ["34","11","50","35","62","44","45","52","46","13","23","41","42","43","15","32","36","22","21","64","63","14","37","31","51","12","54","65","53","33","61","71","81","82","400","100"]
    if brandId_keys is not None:
        timescope = crawlDate+':'+crawlDate
        brand_id = brandId_keys[0]
        keyword = brandId_keys[1]
        if not keyword:
           print '---------------------keyword is nulll'
           return
        ##抓下一个关键词之前先休眠一下，防止被抓
        time.sleep(random.randint(10,30))
        ##实例化收集类，收集指定关键字和起始时间的微博
        cd = CollectMids(keyword,10,brand_id)  
        url = cd.getURL(timescope)
        ##判断是否需要重新划分时间段
        test_url = url + "&page=50"
        cookie_dict,num = getCookie()
        test_data = cd.download(test_url,cookie_dict['cookie'],cookie_dict['name'])
        isCaught,hasMore,mids_size = cd.analysis_data(test_data,crawlDate)
        while isCaught:
            caughtAction(test_url,test_data,num,cookie_dict)
            cookie_dict,num = getCookie()
            test_data = cd.download(test_url,cookie_dict['cookie'],cookie_dict['name'])
            isCaught,hasMore,mids_size = cd.analysis_data(test_data,crawlDate)
        if mids_size < 15:
            ###代表某次请求的结果50页以内可以请求到所有数据
            source_url = url + "&Refer=g"
            data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
            isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
            while isCaught:
                caughtAction(source_url,data,num,cookie_dict)
                cookie_dict,num = getCookie()
                data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
            i = 2
            while hasMore and i < 50 and (not isCaught):
                    ####防止被抓，在请求到第20页的时候换一下cookie
                    if i == 20:
                       cookie_dict,num = getCookie()
                    source_url = url +"&page="+ str(i)
                    data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                    isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
                    while isCaught:
                        caughtAction(source_url,data,num,cookie_dict)
                        cookie_dict,num = getCookie()
                        data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                        isCaught,hasMore,mids_size =cd. analysis_data(data,crawlDate)
                    i+=1
        else:
            ###按小时对该关键词进行重新检索,除2-7,其余时间均为一个小时一个间隔
            m = 0
            while m <24 :
                cookie_dict,num = getCookie()
                if m == 2:
                    timescope = crawlDate + "-2:"+crawlDate + "-7"
                    m=m+6
                else :
                    timescope = crawlDate + "-" + str(m) + ":" + crawlDate + "-"+str(m)
                    m+=1
                url = cd.getURL(timescope)
                ##判断是否需要重新划分区域
                test_url = url + "&page=50"
                test_data = cd.download(test_url,cookie_dict['cookie'],cookie_dict['name'])
                isCaught,hasMore,mids_size = cd.analysis_data(test_data,crawlDate)
                while isCaught:
                    caughtAction(test_url,test_data,num,cookie_dict)
                    cookie_dict,num = getCookie()
                    test_data = cd.download(test_url,cookie_dict['cookie'],cookie_dict['name'])
                    isCaught,hasMore,mids_size = cd.analysis_data(test_data,crawlDate)
                if mids_size < 15:
                    ###代表某次请求的结果50页以内可以请求到所有数据
                    source_url = url + "&Refer=g"
                    data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                    isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
                    while isCaught:
                        caughtAction(source_url,data,num,cookie_dict)
                        cookie_dict,num = getCookie()
                        data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                        isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
                    i = 2
                    while hasMore and i < 50 and (not isCaught):
                        if i == 20:
                            cookie_dict,num = getCookie()
                        source_url = url + "&page=" +str(i)
                        data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                        isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
                        while isCaught:
                            caughtAction(source_url,data,num,cookie_dict)
                            cookie_dict,num = getCookie()
                            data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                            isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
                        i+=1
                else:
                    ###代表某次请求的结果50页不可以请求到所有数据，需要增加按区域检索条件
                    for region in region_list:
                        cookie_dict,num = getCookie()
                        region_url = url + "&region=custom:"+region+":1000"
                        source_url = region_url + "&Refer=g"
                        data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                        isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
                        while isCaught:
                            caughtAction(source_url,data,num,cookie_dict)
                            cookie_dict,num = getCookie()
                            data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                            isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
                        i = 2
                        while hasMore and i < 50 and (not isCaught):
                            if i == 20:
                                cookie_dict,num = getCookie()
                            source_url = region_url +"&page="+ str(i)
                            data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                            isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
                            while isCaught:
                                caughtAction(source_url,data,num,cookie_dict)
                                cookie_dict,num = getCookie()
                                data = cd.download(source_url,cookie_dict['cookie'],cookie_dict['name'])
                                isCaught,hasMore,mids_size = cd.analysis_data(data,crawlDate)
                            i+=1
    else:
        print "------crawler tasks has been finished at" + str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
        ###########增加短信验证码
        hostname = common_utils.get_hostname()
        message_content = 'hostname:' + hostname + ' crawler tasks has been finished at' +str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
        common_utils.send_messag("18561906132",message_content)

def main():
    logger = logging.getLogger('main')
    logFile = './run_collect.log'
    logger.setLevel(logging.DEBUG)
    filehandler = logging.FileHandler(logFile)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)
    while True:
        #####进行昨日数据搜索
        crawlDate = getCrawlDate(4)
        crawlData("weibo:keywords",crawlDate)
    else:
        logger.removeHandler(filehandler)
        logger = None
if __name__ == '__main__':
    main()
