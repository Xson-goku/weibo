import json
import time
import requests
import sys

reload(sys)
sys.setdefaultencoding('utf8')
###使用的是云打码
class YDMHttp:
    apiurl = 'http://api.yundama.com/api.php'
    username = ''
    password = ''
    appid = ''
    appkey = ''

    def __init__(self, name, passwd, app_id, app_key):
        self.username = name
        self.password = passwd
        self.appid = str(app_id)
        self.appkey = app_key

    def request(self, fields, files=[]):
        response = self.post_url(self.apiurl, fields, files)
        response = json.loads(response)
        return response

    def balance(self):
        data = {
            'method': 'balance',
            'username': self.username,
            'password': self.password,
            'appid': self.appid,
            'appkey': self.appkey
        }
        response = self.request(data)
        if response:
            if response['ret'] and response['ret'] < 0:
                return response['ret']
            else:
                return response['balance']
        else:
            return -9001

    def login(self):
        data = {'method': 'login', 'username': self.username, 'password': self.password, 'appid': self.appid,
                'appkey': self.appkey}
        response = self.request(data)
        if response:
            if response['ret'] and response['ret'] < 0:
                return response['ret']
            else:
                return response['uid']
        else:
            return -9001

    def upload(self, filename, codetype, timeout):
        data = {'method': 'upload', 'username': self.username, 'password': self.password, 'appid': self.appid,
                'appkey': self.appkey, 'codetype': str(codetype), 'timeout': str(timeout)}
        file = {'file': filename}
        response = self.request(data, file)
        if response:
            if response['ret'] and response['ret'] < 0:
                return response['ret']
            else:
                return response['cid']
        else:
            return -9001

    def result(self, cid):
        data = {'method': 'result', 'username': self.username, 'password': self.password, 'appid': self.appid,
                'appkey': self.appkey, 'cid': str(cid)}
        response = self.request(data)
        return response and response['text'] or ''

    def decode(self, file_name, code_type, time_out):
        cid = self.upload(file_name, code_type, time_out)
        if cid > 0:
            for i in range(0, time_out):
                result = self.result(cid)
                if result != '':
                    return cid, result
                else:
                    time.sleep(1)
            return -3003, ''
        else:
            return cid, ''

    def post_url(self, url, fields, files=[]):
        for key in files:
            files[key] = open(files[key], 'rb')
        res = requests.post(url, files=files, data=fields)
        return res.text

    def report_error(self, cid):
        data = {
            'method': 'report',
            'username': self.username,
            'password': self.password,
            'appid': self.appid,
            'appkey': self.appkey,
            'flag': 0,
            'cid': cid
        }

        response = self.request(data)
        if response:
            if response['ret']:
                return response['ret']
        else:
            return -9001


def code_verificate(name, passwd, file_name, code_type=1005, app_id=3510, app_key='7281f8452aa559cdad6673684aa8f575',
                    time_out=60):
    yundama_obj = YDMHttp(name, passwd, app_id, app_key)
    cur_uid = yundama_obj.login()
    print 'uid:' + str(cur_uid)

    rest = yundama_obj.balance()
    if rest <= 0:
        print "yundama qian fei	"
    if rest <= 100:
        print "yu e bu zu"
    print 'balance: ' + str(rest)

    cid, result = yundama_obj.decode(file_name, code_type, time_out)
    print 'cid:'+str(cid) + 'result:' +str(result)
    return result, yundama_obj, cid


if __name__ == '__main__':
    username = 'xxx'
    password = 'xxx'

    rs = code_verificate(username, password, 'pincode.png')
