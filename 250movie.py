import requests
from bs4 import BeautifulSoup
import time


url = 'https://movie.douban.com/top250'
headers = {
    'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
}
name = {}
for x in range(0, 10):
    number = x*25
    params = {
        'start': number,
        'filter': ''
    }
    wb_data = requests.get(url,headers=headers,params=params)
    if wb_data == 200:
        print('正在下载第 %s 页' % (x+1))
    items = BeautifulSoup(wb_data.content,'lxml')
    movie_name = items.find('ol',class_="grid_view").find_all('li')
    for i in movie_name:
        dict1 = {'quote': '', 'start': '', 'people': '','url': ''}  # 每次循环重新定义一个字典 dict1
        move_url = i.find('a',class_='').get('href')
        start = i.find('span', class_="rating_num").get_text()
        people = i.find('div', class_="star").find_all('span')[3].get_text()
        try:
            quote = i.find('p',class_="quote").get_text().strip('\n')  # 如果没有引言，quote = ''
        except AttributeError:
            quote = ''
        dict1['start'] = start
        dict1['people'] = people
        dict1['quote'] = quote
        dict1['url'] = move_url
        name[(i.find('span', class_="title").get_text())] = dict1
    time.sleep(1)

print(name)
