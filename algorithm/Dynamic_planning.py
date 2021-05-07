#-*- coding:utf-8 -*-
#Author:李明洲
#python 3.7.3
#@Time:2020/6/11 19:08

#python 数据结构与算法 学到的一些动态规划

#反复多看几次
# def printcoin(coinUsed,change):
#     coin = change
#     while coin > 0:
#         thisCoin = coinUsed[coin]
#         print(thisCoin)
#         coin = coin - thisCoin
#
# def reMC(coinList,change,coinUsed):
#     dp=[0]*(change+1)
#
#     for cent in range(1,len(dp)):
#         coincount = cent
#         newCoin = 1
#         for j in [c for c in coinList if c <= cent]:
#             if dp[cent-j] + 1 < coincount:
#                 coincount = dp[cent-j] + 1
#                 newCoin = j
#         dp[cent] = coincount
#         coinUsed[cent] = newCoin
#     return dp[-1]
#
# coinused = [0]*64
# print(reMC([1,5,10,21,25],63,coinused))
# print(coinused)


#背包dp问题
#二维数组dp求解  （i,w)  表示拿i件宝物，使用重量w情况下的最多钱

#拿0件宝物重量和钱都为0 直接设置为None
tr = [None, {'w' : 2,'v':3},{'w':3,'v':4},
            {'w' : 4,'v':8},{'w':5,'v':8},
            {'w' : 9,'v':10}]
max_w = 20
#生成动态规划dps数组
m = {(i,w):0 for i in range(len(tr))
            for w in range(max_w + 1)}
for i in range(1,len(tr)):
    for w in range(1,max_w + 1):
        #背包重量不足以拿第i个宝物
        if tr[i]['w'] > w:
            m[(i,w)] = m[(i-1,w)]
        else:
            #够拿第i个宝物
            #相当于拿这个宝物和
            m[(i,w)] = max(m[(i-1,w)],m[(i-1,w-tr[i]['w'])]+tr[i]['v'])
print(m[(len(tr)-1,max_w)])


