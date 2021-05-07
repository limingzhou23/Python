'''
-*- coding: utf-8 -*-
@Author  : 李明洲
@Time    : 2020/7/1 21:22
@Software: PyCharm
@File    : demo.py
'''
from bs4 import BeautifulSoup
import json
import requests
from requests.exceptions import RequestException
import re
import time

# def get_one_page(url):
#     try:
#         response = requests.get(url)
#         if response.status_code == 200:
#             return response.text
#         return None
#     except RequestException:
#         print('error')
#         return None
#
# def parse_one_page(html):
#     pattern = re.compile('<dd>.*?board-index.*?>(\d+)</i>.*?data-src="(.*?)".*?name"><a'
#                          + '.*?>(.*?)</a>.*?star">(.*?)</p>.*?releasetime">(.*?)</p>'
#                          + '.*?integer">(.*?)</i>.*?fraction">(.*?)</i>.*?</dd>', re.S)
#     print(pattern)
#     items = re.findall(pattern, html)
#     for item in items:
#         yield {
#             'index': item[0],
#             'image': item[1],
#             'title': item[2],
#             'actor': item[3].strip()[3:],
#             'time': item[4].strip()[5:],
#             'score': item[5] + item[6]
#         }
#
# def write_to_file(content):
#     with open('result.txt', 'a', encoding='utf-8') as f:
#         f.write(json.dumps(content, ensure_ascii=False) + '\n')
#
# def main(offset):
#     url = 'http://maoyan.com/board/4?offset=' + str(offset)
#     html = get_one_page(url)
#     for item in parse_one_page(html):
#         write_to_file(item)
#
if __name__ == '__main__':
    target = 'http://www.biqukan.com/1_1094/5403177.html'
    req = requests.get(url=target)
    html = req.text
    bf = BeautifulSoup(html)
    texts = bf.find_all('div', class_='showtxt')
    print(texts[0].text.replace('\xa0' * 8, '\n\n'))

