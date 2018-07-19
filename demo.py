import requests
from multiprocessing.dummy import Pool as ThreadPool
import time
import random
import json
import pymysql
import datetime
import re

'''
多线程爬取微博上搜索关键词信息(根据关键词如"刷单","淘宝信誉","银行卡","代办信用卡","返现","返利"搜索)

'''

def weibo_crawl(url):

    headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'Host': 'm.weibo.cn',
    'MWeibo-Pwa': '1',
    'Referer': 'https://m.weibo.cn/p/searchall?containerid=100103type%3D1%26q%3D%E6%B7%98%E5%AE%9D%E5%88%B7%E5%8D%95',
    'User-Agent': 'Mozilla/5.0 (Linux;u;Android 4.2.2;zh-cn;) AppleWebKit/534.46 (KHTML,like Gecko) Version/5.1 Mobile Safari/10600.6.3 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
    'X-Requested-With': 'XMLHttpRequest',
    }
    try:

        r = requests.get(url=url, headers=headers, timeout=1.5)
        resp = r.text
        results = json.loads(resp)
        info_list_temp = results["data"]["cards"]
        #个别page会没有内容要判断处理下
        if info_list_temp == []:

            pass
        else:

            info_list = info_list_temp[-1]["card_group"]
            for info in info_list:
                
                source = "新浪微博"
                account_id = info["user"]["id"] #账号id
                account_nickname = info["user"]["screen_name"] #个人账号昵称
                account_url = info["user"]["profile_url"] #个人账号主页地址
                description_temp = info["desc1"].strip().replace(' ','') #个人简介
                #这里的description_temp是因为有的没有简介,没有的抓到了粉丝数,这边加个判断去掉
                if "粉丝" in description_temp:

                    description = ""                         
                else:

                    description = description_temp
                
                #从用户列表接口返回的数据中得到了uid,去请求个人主页接口取微博内容,和
                #每个内容里的评论数,有就采,没有就不采

                account_first_url = "https://m.weibo.cn/api/container/getIndex"
                params = {
                "type": "uid",
                "value": str(account_id),
                "containerid":  "107603" + str(account_id),
                }
                res = requests.get(url=account_first_url, headers=headers, params=params, timeout=2)
                res_dict = json.loads(res.text)
                if res_dict["ok"] == 1:

                    if "cardlistInfo" in res_dict["data"]:           

                        total = res_dict["data"]["cardlistInfo"]["total"]
                        max_page = int(total/10+1)
                        for k in range(1, max_page+1):
                            
                            account_api = "https://m.weibo.cn/api/container/getIndex"
                            querystring = {
                            "type": "uid",
                            "value": str(account_id),
                            "containerid":  "107603" + str(account_id),
                            "page": str(k)
                            }
                            r = requests.get(url=account_api, headers=headers, params=querystring, timeout=2.5)
                            if r.status_code == 200:

                                res = r.text
                                if res:

                                    results = json.loads(res)
                                    if results["ok"] == 1:
                                        
                                        if "cards" in results["data"]:
                                            comments_temp_counts = results["data"]["cards"] 
                                            for item in comments_temp_counts:
                                                
                                                description = description #简介
                                                account_url = account_url #个人主页地址
                                                account_nickname = account_nickname #个人账号昵称
                                                content_temp = item["mblog"]["text"]
                                                pattern = re.compile(r'<[^>]+>') #正则去掉标签内容
                                                content = pattern.sub('', content_temp) #微博正文
                                                publish_time_temp = item["mblog"]["created_at"] #微博正文发布时间
                                                if len(publish_time_temp) < 6:
                                                    now_year_temp = datetime.datetime.today()
                                                    now_year = datetime.datetime.strftime(now_year_temp, "%Y-")
                                                    publish_time = now_year + publish_time_temp 
                                                else:
                                                    publish_time = publish_time_temp
                                                crawl_time = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') #爬取时间
                                                comments_count = item["mblog"]["comments_count"] #根据评论数是否为零去爬评论
                                                mid = item["mblog"]["id"]    #mid和id是获取评论第一条接口的主要参数,后面的评论还需要另外一个max_id
                                                if comments_count and mid:

                                                    if comments_count == 0:

                                                        comments = ""
                                                        comments_nickname = ""
                                                    else:

                                                        #去爬评论接口,这里注意下headers里的referer要改下,不然没数据
                                                        comments_first_api = "https://m.weibo.cn/comments/hotflow"
                                                        new_headers = {
                                                        'Accept': 'application/json, text/plain, */*',
                                                        'Accept-Encoding': 'gzip, deflate, br',
                                                        'Accept-Language': 'zh-CN,zh;q=0.9',
                                                        'Connection': 'keep-alive',
                                                        'Host': 'm.weibo.cn',
                                                        'MWeibo-Pwa': '1',
                                                        'Referer': 'https://m.weibo.cn/status/' + mid,
                                                        'User-Agent': 'Mozilla/5.0 (Linux;u;Android 4.2.2;zh-cn;) AppleWebKit/534.46 (KHTML,like Gecko) Version/5.1 Mobile Safari/10600.6.3 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
                                                        'X-Requested-With': 'XMLHttpRequest',
                                                        }
                                                        params = {"id":mid,"mid":mid,"max_id_type":"0"}
                                                        #time.sleep(random.random())
                                                        r = requests.get(url=comments_first_api, headers=new_headers, params=params, timeout=3)
                                                        if r.status_code == 200:
                                                            reps = json.loads(r.text)
                                                            if reps:
                                                                
                                                                if "data" in reps:

                                                                    comments_info_list = reps["data"]["data"] #comments_info是个评论信息列表里面有评论内容和评论人评论时间等
                                                                    for comments_info in comments_info_list:
                                                                    
                                                                        comments_temp =  comments_info["text"]
                                                                        pattern = re.compile(r'<[^>]+>') #微博表情是span标签内的,去除无用的表情标签
                                                                        comments = pattern.sub('', comments_temp) #评论内容
                                                                        #comments_time = comments_info["created_at"] 
                                                                        comments_nickname = comments_info["user"]["screen_name"] #评论人昵称
                                                    '''
                                                    在最后一层获取评论接口入库
                                                    '''
                                                    try:
                                                        db = pymysql.connect(host='', port=3306, user='root', passwd='', db='', use_unicode=True, charset="utf8",cursorclass=pymysql.cursors.DictCursor)
                                                        cursors = db.cursor()
                                                        sql = """insert into sjk(source, account_url, account_nickname, description, content, comments, crawl_time, publish_time, comments_nickname) values(source, account_url, account_nickname, description, content, comments, crawl_time, publish_time, comments_nickname)"""
                                                        cursors.execute(sql)
                                                        db.commit()
                                                    except Exception as e:
                                                        print(e)
                                                    finally:
                                                        db.close()             
    except requests.exceptions.RequestException as e:
        print(e)

if __name__ == '__main__':
    url_list = []
    url_sd_list = []
    url_tbxy_list = []
    url_yhk_list = []
    url_dbxyk_list = []
    url_fx_list = []
    url_fl_list = []
    for i in range(1,51):
        url_tbxy = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D3%26q%3D%E6%B7%98%E5%AE%9D%E4%BF%A1%E8%AA%89%26t%3D0&page_type=searchall&page={}'.format(i)
        url_tbxy_list.append(url_tbxy)
        url_sd = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D3%26q%3D%E5%88%B7%E5%8D%95%26t%3D0&page_type=searchall&page={}'.format(i)
        url_sd_list.append(url_sd)
        url_dbxyk = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D3%26q%3D%E4%BB%A3%E5%8A%9E%E4%BF%A1%E7%94%A8%E5%8D%A1%26t%3D0&page_type=searchall&page={}'.format(i)
        url_dbxyk_list.append(url_dbxyk)
        url_yhk = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D3%26q%3D%E9%93%B6%E8%A1%8C%E5%8D%A1%26t%3D0&page_type=searchall&page={}'.format(i)
        url_yhk_list.append(url_yhk)
        url_fx = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D3%26q%3D%E8%BF%94%E7%8E%B0%26t%3D0&page_type=searchall&page={}'.format(i)
        url_fx_list.append(url_fx)
        url_fl = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D3%26q%3D%E8%BF%94%E5%88%A9%26t%3D0&page_type=searchall&page={}'.format(i)
        url_fl_list.append(url_fl)
    url_list.extend(url_sd_list)
    url_list.extend(url_tbxy_list)
    url_list.extend(url_dbxyk_list)
    url_list.extend(url_yhk_list)
    url_list.extend(url_fl_list)
    url_list.extend(url_fx_list)
    pool = ThreadPool(3)
    pool.map(weibo_crawl, url_list)
    pool.close()
    pool.join()
