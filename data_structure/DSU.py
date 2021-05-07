#-*- coding:utf-8 -*-
#Author:李明洲
#python 3.7.3
#@Time:2020/6/6 10:39

#并查集
import collections

class DSU:
    def __init__(self, nums):
        self.pre = {num: num for num in nums}
        self.rank = collections.defaultdict(lambda: 1)
        self.cnt = collections.defaultdict(lambda: 1)
    def find(self, x):
        while x != self.pre[x]:
            x = self.pre[x]
        return x
    def merge(self, x, y):
        if y not in self.pre:
            return 1
        root1, root2 = self.find(x), self.find(y)
        if root1 == root2:
            return self.cnt[root1]
        if self.rank[root1] < self.rank[root2]:
            self.pre[root1] = root2
            self.cnt[root2] += self.cnt[root1]
            return self.cnt[root2]
        elif self.rank[root1] > self.rank[root2]:
            self.pre[root2] = root1
            self.cnt[root1] += self.cnt[root2]
            return self.cnt[root1]
        else:
            self.pre[root1] = root2
            self.cnt[root2] += self.cnt[root1]
            self.rank[root2] += 1
            return self.cnt[root2]