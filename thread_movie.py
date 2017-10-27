#!-*- UTF-8 -*-
import requests
import pymysql
from lxml import etree
import re
import datetime
import threading
import queue
import time
from random import choice


def sql_link():  # 链接数据库
    connect = pymysql.Connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='790919',
        db='my_text',
        charset='utf8'
    )
    print('连接数据库完成')
    cursor = connect.cursor()  # 获取游标
    return cursor,connect



def html_need():  # 请求头
    url = 'https://movie.douban.com/j/new_search_subjects'
    proxies = [
        {'http': 'http://111.79.82.138:9756', 'https': 'http://111.79.82.138:9756'},
        {'http': 'http://182.118.98.183:2481', 'https': 'http://182.118.98.183:2481'},
        {'http': 'http://182.43.215.218:1629', 'https': 'http://182.43.215.218:1629'},
        {'http': 'http://60.189.155.168:53936', 'https': 'http://60.189.155.168:53936'},
        {'http': 'http://115.229.25.246:1728', 'https': 'http://115.229.25.246:1728'},
                ]

    starlist = ['8.6,9']

    return url, starlist, proxies


class Movie(threading.Thread):
    def __init__(self):
        super(Movie, self).__init__()

    def run(self):
        get_more()


def get_all(params):

    headers1 = {
        'Accept':'application/json, text/plain, */*',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Referer':'https://movie.douban.com/tag/',
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
    }
    global q
    html = requests.get(url, params=params, headers=headers1).json()
    x = html.get('data')
    if x != []:
        for items in x:
            ids = items.get('id')
            if ids not in see_id:
                see_id[items.get('id')] = ''
                sub_url = items.get('url')
                q.put(sub_url)

        for t in range(7):
            movie = Movie()
            movie.start()
            t_list.append(movie)

        for t in t_list:
            t.join()
    else:
        boom.append(0)


def get_more():  # 点进页面电影详情

    headers2 = {
        'Accept':'application/json, text/plain, */*',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Referer':'https://movie.douban.com/tag/',
        'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'}

    global q
    proxies = choice(pros)
    while not q.empty():
        sub_url1 = q.get()
        try:
            html1 = requests.get(sub_url1, headers=headers2,proxies=proxies,timeout=15)
        except requests.exceptions.ProxyError:
            pros.remove(proxies)
            error_list.append(sub_url1)
            proxies = choice(pros)
            html1 = requests.get(sub_url1, headers=headers2, proxies=proxies, timeout=15)


        wb = etree.HTML(html1.content)
        list_dict = dict()

        string = sub_url1
        movie_id = string.split('/')[-2]
        name = wb.xpath('//*[@id="content"]/h1/span[@property="v:itemreviewed"]/text()')[0]
        directors = wb.xpath('//*[@id="info"]/span[1]/span[@class="attrs"]/a[@rel="v:directedBy"]/text()')
        list_dict['name'] = name
        casts = re.findall(r'rel="v:starring".(.*?)</a>', html1.text)
        language = re.findall(r'语言.</span> (.*?)<br', html1.text)
        area = re.findall(r'地区.</span> (.*?)<br', html1.text)
        movie_type = wb.xpath('//*[@id="info"]/span[@property="v:genre"]/text()')
        day_data = wb.xpath('//*[@id="info"]/span[@property="v:initialReleaseDate"]/@content')
        movie_time = wb.xpath('//*[@id="info"]/span[@property="v:runtime"]/@content')
        movie_imdb = re.findall('IMDb链接:</span> <a href=(.*?)target', html1.text)
        star = wb.xpath('//*[@id="interest_sectl"]/div[1]/div[2]/strong/text()')[0]
        people = wb.xpath('//*[@id="interest_sectl"]/div[1]/div[2]/div/div[2]/a/span[@property="v:votes"]/text()')[0]

        list_dict['directors'] = get_text(directors)
        list_dict['star'] = star
        list_dict['casts'] = get_text(casts)
        list_dict['type'] = get_text(movie_type)
        list_dict['area'] = get_text(area)
        list_dict['language'] = get_text(language)
        list_dict['daytime'] = get_text(day_data)
        list_dict['movie_time'] = get_text(movie_time)
        list_dict['imdb_url'] = get_imdb_url(movie_imdb)
        list_dict['people'] = people

        id_dict[movie_id] = list_dict
        time.sleep(6)
        print('爬取   %s '% name)
        # print(name, 'i\'m %s' % threading.current_thread().getName())


def get_imdb_url(x):
    if x:
        url2 = eval(x[0])

        return url2


def get_text(item):
    text = '/'.join(item)
    return text


if __name__ == '__main__':
    see_id = {}  # 定义字典去重用
    url, starlist, pros = html_need()  # 附值
    cursor, connect = sql_link()  # 得到游标
    q = queue.Queue()
    error_list = []
    boom = []
    for rank in starlist:
        print('正在下载区间为   %s' % rank)
        boom = []
        for i in range(1, 501):
            t_list = []

            id_dict = dict()
            begin = datetime.datetime.now()
            num = i * 20
            payload = {
                'sort': 'T',
                'range': rank,
                'tags': '电影',
                'start': num
            }

            get_all(params=payload)
            if boom != []:
                break
            end = datetime.datetime.now()
            print('完成 %s 页耗时       %s' % ((i+1), (end - begin)))
            for key,value in id_dict.items():
                try:
                    sql_2 = '''
                        INSERT INTO doubanmovie.movie(id,name,type,star,people,directors,casts,area,language,daytime,
                        movie_time,IMDb_url) VALUE (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                        )
                    '''

                    a = [key, value['name'], value['type'], value['star'], value['people'], value['directors'],
                         value['casts'], value['area'], value['language'], value['daytime'], value['movie_time'],
                         value['imdb_url']]
                    cursor.execute(sql_2, a)
                    connect.commit()
                except pymysql.err.IntegrityError:
                    pass

            print('存入数据库完毕       %s' % rank)


    cursor.close()
    connect.close()
    print(error_list)