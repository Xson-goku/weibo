# coding:utf-8
import datetime
import json
import redis
import time
import login
from config.conf import get_redis_args
import common_utils

redis_args = get_redis_args()


class Cookies(object):
    rd_con = redis.StrictRedis(host=redis_args.get('host'), port=redis_args.get('port'),
                               password=redis_args.get('password'), db=redis_args.get('cookies'))

    rd_con_broker = redis.StrictRedis(host=redis_args.get('host'), port=redis_args.get('port'),
                                      password=redis_args.get('password'), db=redis_args.get('broker'))

    @classmethod
    def store_cookies(cls, name, cookies):
        picked_cookies = json.dumps(
            {'cookies': cookies, 'loginTime': str(int(round(time.time()*1000))),'name':name})
        cls.rd_con.sadd('set:cookies', picked_cookies)
    
    @classmethod
    def store_invalid_name(cls, name):
        picked_name = json.dumps(
            {'invalid_time': str(int(round(time.time()*1000))),'name':name})
        cls.rd_con.sadd('set:invalid:account', picked_name)

    @classmethod
    def fetch_cookies(cls):
        cookies_json = cls.rd_con.spop('set:cookies')
        while cookies_json is None :
            ###login.get_session()
            time.sleep(600)
            print '!!!!!!!!!cookie none'
            message_content = 'cookie none'
            common_utils.send_messag("18561906132",message_content)
            cookies_json = cls.rd_con.spop('set:cookies')
        cookies = json.loads(cookies_json.decode())['cookies']
        name = json.loads(cookies_json.decode())['name']
        print '----name:'+name
        return cookies,name
    ###每次从redis中获取五个cookie
    @classmethod
    def getCookiesList(self):
       i = 0;
       cookies_list =[]
       while i <5:
         cookie_dict = {}
         cookies,name = self.fetch_cookies()
         cookie_dict['cookie']=cookies
         cookie_dict['name']=name
         cookie_dict['err_times']=0
         cookies_list.append(cookie_dict)
         i = i +1
       return cookie_list


class Login_info(object):
    rd_con = redis.StrictRedis(host=redis_args.get('host'), port=redis_args.get('port'),
                               password=redis_args.get('password'), db=redis_args.get('cookies'))
    @classmethod
    def store_account(cls, name, password):
        picked_info = json.dumps(
            {'name': name, 'password': password})
        cls.rd_con.sadd('set:account',picked_info )

    @classmethod
    def fetch_account(cls):
        account_json = cls.rd_con.spop('set:account')
        while account_json is None :
            ####加发送短信验证码
            print '----redis中可用可用账号不足，请尽快处理'

            time.sleep(600)
            account_json = cls.rd_con.spop('set:account')
        password = json.loads(account_json.decode())['password']
        name = json.loads(account_json.decode())['name']
        return name,password


class Keywords(object):
    rd_con = redis.StrictRedis(host=redis_args.get('host'), port=redis_args.get('port'),
                               password=redis_args.get('password'), db=redis_args.get('urls'))
    ###存储关键词和brand_id
    @classmethod
    def store_keywords(cls, brand_id, keyword):
        picked_keyword = json.dumps(
            {'brand_id': brand_id, 'keyword': keyword})
        cls.rd_con.sadd('weibo:keywords', picked_keyword)
	###获取一个关键词
    @classmethod
    def fetch_keyword(cls,set_name):
        keyword_json = cls.rd_con.spop(set_name)
        while keyword_json is None :
            ####加发送短信验证码
            print '----keyword已空，请尽快确认爬虫任务是否完成'
            if set_name is 'set:keywords:unsearch':
                return None
            time.sleep(600)
            keyword_json = cls.rd_con.spop(set_name)
        keyword = json.loads(keyword_json.decode())['keyword']
        brand_id = json.loads(keyword_json.decode())['brand_id']
        return brand_id,keyword
	#####将当日未搜索的所有关键词插入到历史set中
    @classmethod
    def move_keywords(cls):
        num = cls.rd_con.scard('weibo:keywords')
        i = 0 
        while i < num:
            keyword_json = cls.rd_con.spop('weibo:keywords')
            cls.rd_con.sadd('weibo:keywords:unsearch',keyword_json)
            i=i+1
   	#####将当日未搜索的所有关键词插入到历史set中
    @classmethod
    def scard(cls,setname):
        num = cls.rd_con.scard(setname)
        return num


class Mids(object):
    rd_con = redis.StrictRedis(host=redis_args.get('host'), port=redis_args.get('port'),
                               password=redis_args.get('password'), db=redis_args.get('urls'))
    ###存储mid
    @classmethod
    def store_mid(cls, brand_id, mid,crawl_date):
        picked_mid = json.dumps(
            {'brand_id': brand_id, 'mid': mid})
        i = cls.rd_con.sadd('weibo:mids:'+crawl_date, picked_mid)
        print '-----i:'+str(i)+',mid:'+mid
        if i>0:
             j=cls.rd_con.sadd('weibo:mids', picked_mid)
             print '----j:'+str(j)
    ###获取一个mid
    @classmethod
    def fetch_mid(cls,set_name):
        mid_json = cls.rd_con.spop(set_name)
        if mid_json is None :
            ####加发送短信验证码
            print '----mid已空，请尽快确认爬虫任务是否完成'
            return None
        mid = json.loads(mid_json.decode())['mid']
        brand_id = json.loads(mid_json.decode())['brand_id']
        return brand_id,mid
    #####将当日未重新爬取的所有微博id插入到历史set中
    @classmethod
    def move_mids(cls):
        num = cls.rd_con.scard('weibo:mids')
        i = 0 
        while i < num:
           mid_json = cls.rd_con.spop('weibo:mids')
           cls.rd_con.sadd('weibo:mids:uncrawled',mid_json)
    ####获取指定日期
    @classmethod
    def getCrawlDate(num): 
        today=datetime.date.today() 
        delta=datetime.timedelta(days=num) 
        crawlDate=today-delta  
        return str(crawlDate)
    
    ###将先前6天的mids放入set:mids中
    @classmethod
    def mids_unionstore(cls,set7,set0,set1,set2,set3,set4,set5,set6):
        cls.rd_con.sunionstore(set7,set0,set1,set2,set3,set4,set5,set6)
def main():
    cd = Key_words()
    cd.move_keyword()
if __name__ == '__main__':
    main()
