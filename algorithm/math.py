#-*- coding:utf-8 -*-
#Author:李明洲
#@Time:2020/6/4 21:09

import time
def eratosthenes(n):
    primes = []
    is_prime = [True] * (n + 1)
    for i in range(2, n+1):
        if is_prime[i]:
            primes.append(i)
            # 用当前素数i去筛掉所有能被它整除的数
            for j in range(i * 2, n+1, i):
                is_prime[j] = False
    return primes


def ertosthenes(n):
    primes = []
    is_prime = [True] * (n+1)

    for i in range(2, n+1):
        if is_prime[i]:
            primes.append(i)
        for j, p in enumerate(primes):
            # 防止越界
            if p > n // i:
                break
            # 过滤
            is_prime[i * p] = False
    # 当i % p等于0的时候说明p就是i最小的质因数
            if i % p == 0:
                break

    return primes

t1=time.clock()
print(ertosthenes(100))
t2=time.clock()
print(eratosthenes(100))
t3=time.clock()
print(t2-t1)
print(t3-t2)


# class Solution(object):
#     def queensAttacktheKing(self, queens, king):
#         """
#         :type queens: List[List[int]]
#         :type king: List[int]
#         :rtype: List[List[int]]
#         """
#         attacks = []
#         direction=[[-1,-1],[-1,0],[-1,1],
#                     [0,-1],[0,1],
#                    [1,-1],[1,0],[1,1]]
#         for dir in direction:
#             temp = king.copy()
#             temp[0] += dir[0]
#             temp[1] += dir[1]
#             while 8>temp[0] >-1 and 8>temp[1]>-1:
#                 if temp in queens:
#                     attacks.append(temp)
#                     break
#                 else:
#                     temp[0] += dir[0]
#                     temp[1] += dir[1]
#         return attacks
#
# queens = [[0,1],[1,0],[4,0],[0,4],[3,3],[2,4]]
# king = [0,0]