'''
-*- coding: utf-8 -*-
@Author  : 李明洲
@Time    : 2020/7/14 21:14
@Software: PyCharm
@File    : cache.py
'''
#python缓存机制学习

import datetime
import random
import time

class MyLruCache:
    def __init__(self):
        self.cache = {}
        self.max_cache_size = 5

    def __contains__(self, key):
        return  key in self.cache

    def get_cache_size(self):
        return self.max_cache_size

    #删除最早的条目
    def update_cache(self,key,value):
        #当缓存满了，需要添加新东西就移除最早的
        if key not in self.cache and self.max_cache_size<= len(self.cache):
                self.remove_oldest()
        self.cache[key] = {"date":time.time(),"val":value}
    #找到一个字典里面最早的数据
    def remove_oldest(self):
        oldest=None
        for k in self.cache:
            if not oldest:
                oldest = k
            elif self.cache[k]["date"] < self.cache[oldest]["date"]:
                oldest = k
        self.cache.pop(oldest)

if __name__ == "__main__":
    keys = ['test', 'red', 'fox', 'fence', 'junk', 'other']
    cache = MyLruCache()
    for i, key in enumerate(keys):
        if key in cache:
            continue
        else:
            value = key
            time.sleep(1)
            cache.update_cache(key, value)

    for i in cache.cache.items():
        print(i)
