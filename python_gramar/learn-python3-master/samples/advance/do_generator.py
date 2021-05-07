#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# s = (x * x for x in range(5))
# print(s)
# for x in s:
#     print(x)
#
# def fib(max):
#     n, a, b = 0, 0, 1
#     while n < max:
#         yield b
#         a, b = b, a + b
#         n = n + 1
#     return 'done'
#
# f = fib(10)
# print('fib(10):', f)
# for x in f:
#     print(x)
#
# # call generator manually:
# g = fib(5)
# while 1:
#     try:
#         x = next(g)
#         print('g:', x)
#     except StopIteration as e:
#         print('Generator return value:', e.value)
#         break
def d():
    print('初始化')
    sum = 0
    value = yield sum
    sum = sum + value
    print('sum的值是：%d' % sum)
    value = yield sum
    sum = sum + value
    print('sum的值是：%d' % sum)
    value = yield sum
    sum = sum + value
    print('sum的值是：%d' % sum)
    return sum+1


c = d()          # c是一个生成器，此行代码并不运行d()内容
a = c.send(None)
while True:
    print('生成器传出的值:%d' % a)
    try:
        a = c.send(1)
    except StopIteration as e:
        print('生成器传出最后的值:%d' % e.value)
        break