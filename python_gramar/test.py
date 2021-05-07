'''
-*- coding: utf-8 -*-
@Author  : 李明洲
@Time    : 2020/6/29 14:58
@Software: PyCharm
@File    : test.py
'''

import  sys
#一些关键词
#assert 用于判断一个表达式，在表达式条件为 false 的时候触发异常
#assert 1==2

#raise 自己触发异常
# def mye( level ):
#     if level < 1:
#         raise Exception("Invalid level!")
#         # 触发异常后，后面的代码就不会再执行
#
# try:
#     mye(0)            # 触发异常
# except Exception as err:
#     print(1,err)
# else:
#     print(2)

####################################################################
######################################################################
#迭代器和生成器
# 迭代器是一个可以记住遍历的位置的对象。
# 迭代器对象从集合的第一个元素开始访问，直到所有的元素被访问完结束。迭代器只能往前不会后退
#迭代器有两个基本的方法：iter() 和 next()
#字符串，列表或元组对象都可用于创建迭代器
# list = [1,2,3,4]
# it = iter(list)
# print(it)
#
# #迭代器对象可以使用常规for语句进行遍历：
# # for x in it:
# #     print(x,end= " ")
#
# #可以使用 next() 函数
# while True:
#     try:
#         print(next(it))
#     except StopIteration:
#         sys.exit()
#把一个类作为一个迭代器使用需要在类中实现两个方法 __iter__() 与 __next__()
#__iter__() 方法返回一个特殊的迭代器对象， 这个迭代器对象实现了
# __next__() 方法并通过 StopIteration 异常标识迭代的完成
# class MyNumbers:
#     def __iter__(self):
#         self.a = 1
#         return self
#
#     def __next__(self):
#         if self.a <= 20:
#             x = self.a
#             self.a += 1
#             return x
#         else:
#             raise StopIteration
#
#
# myclass = MyNumbers()
# myiter = iter(myclass)
#
# for x in myiter:
#     print(x)

####################################################################
######################################################################
# 使用了 yield 的函数被称为生成器（generator）。
# 跟普通函数不同的是，生成器是一个返回迭代器的函数，只能用于迭代操作，
# 更简单点理解生成器就是一个迭代器。
# 在调用生成器运行的过程中，每次遇到 yield 时函数会暂停并保存当前所有的运行信息，返回 yield 的值,
# 并在下一次执行 next() 方法时从当前位置继续运行。
# 调用一个生成器函数，返回的是一个迭代器对象。

# def fibonacci(n):  # 生成器函数 - 斐波那契
#     a, b, counter = 0, 1, 0
#     while counter <= n:
#         yield a
#         a, b = b, a + b
#         counter += 1
# f = fibonacci(10)  # f 是一个迭代器，由生成器返回生成
#
# while True:
#     try:
#         print(next(f), end=" ")
#     except StopIteration:
#         sys.exit()

####################################################################
######################################################################
#可变参数传递 *args,**kwargs
#*代表元组形式输入,**代表字典
def x(arg, *args, **kwargs):
    print('arg--> {}'.format(arg))
    print('args--> {} '.format(args))
    print('kwarg--> {} '.format(kwargs))
#x(1, 2, 3, 4, key1="value1")

#**kwargs接收关键字参数，生成字典。
# *args接收除第一个位置参数和关键字参数外的其它参数生成元组，跟参数是什么数据类型无关
def y(arg, *args, **kwargs):
    print('arg--> {}'.format(arg))
    print('args--> {} '.format(args))
    print('kwarg--> {} '.format(kwargs))
#y(1, (2, 3,), 4, key1="value1")

#函数调用时的使用
def print_info(name, age, sex):
    print('name--> {}'.format(name))
    print('age--> {} '.format(age))
    print('sex--> {} '.format(sex))

list = ["李刚", 20, "男"]
dic = {"name": "王某某", "age": 30, "sex": "女", }
print_info(*list)
print_info(**dic)
print_info("小刚",age=18,**{"sex":"男"})

####################################################################
######################################################################
#format 使用
print("{} {}".format("hello", "world"))  # 不设置指定位置，按默认顺序
print("{0} {1}".format("hello", "world"))  # 设置指定位置
print("{1} {0} {1}".format("hello", "world"))  # 设置指定位置

print("网站名：{name}, 地址 {url}".format(name="菜鸟教程", url="www.runoob.com"))
# 通过字典设置参数
site = {"name": "菜鸟教程", "url": "www.runoob.com"}
print("网站名：{name}, 地址 {url}".format(**site))
# 通过列表索引设置参数
my_list = ['菜鸟教程', 'www.runoob.com']
print("网站名：{0[0]}, 地址 {0[1]}".format(my_list))  # "0" 是必须的

#也可以传入对象
class AssignValue(object):
    def __init__(self, value):
        self.value = value
my_value = AssignValue(6)
print('value 为: {0.value}'.format(my_value))  # "0" 是可选的

#数字格式化
print("{:.2f}".format(3.1415926))
print('{:b}'.format(11))
print('{:d}'.format(11))
print('{:o}'.format(11))
print('{:x}'.format(11))
print('{:#x}'.format(11))
print('{:#X}'.format(11))

#可以使用大括号 {} 来转义大括号，如下实例
print ("{} 对应的位置是 {{0}}".format("runoob"))





