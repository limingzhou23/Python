'''
-*- coding: utf-8 -*-
@Author  : 李明洲
@Time    : 2020/6/30 15:13
@Software: PyCharm
@File    : found.py
'''
#一些基础知识
import os
import sys
from functools import wraps

#argv函数简介
#Sys.argv[]是用来获取命令行参数的，sys.argv[0]表示代码本身文件路径，所以参数从1开始，
# script = sys.argv
# print("the script is called:", script)
# for i in sys.argv:
#     print(i)
# print(len(sys.argv))


#利用sys.argv[] 进行脚本控制
# def readfile(filename): #从文件中读出文件内容
#     '''''Print a file to the standard output.'''
#     f = open(filename)
#     while True:
#         line = f.readline()
#         if len(line) == 0:
#             break
#         print(line) # notice comma 分别输出每行内容
#     f.close()
# if __name__ == "__main__":
#     print(sys.argv)
#     if len(sys.argv) < 2:
#         print('No action specified.')
#         sys.exit()
#
#     if sys.argv[1].startswith('--'):
#         option = sys.argv[1][2:]
#         # fetch sys.argv[1] but without the first two characters
#         if option == 'version': #当命令行参数为-- version，显示版本号
#             print('Version 1.2')
#         elif option == 'help': #当命令行参数为--help时，显示相关帮助内容
#             print('''''/ This program prints files to the standard output.
#                         Any number of files can be specified.
#                         Options include:
#                         --version : Prints the version number
#                         --help : Display this help''')
#         else:
#             print('Unknown option.')
#             sys.exit()
#     else:
#         for filename in sys.argv[1:]: #当参数为文件名时，传入readfile，读出其内容
#             readfile(filename)


# 装饰器(Decorators)是 Python 的一个重要部分。
# 简单地说：他们是修改其他函数的功能的函数。 在不改变原有功能代码的基础上,添加额外的功能,如用户验证等
# 他们有助于让我们的代码更简短，也更Pythonic
# def hi(name="yasoob"):
#     return "hi " + name
# print(hi())
# # 我们甚至可以将一个函数赋值给一个变量，比如
# greet = hi
# # 我们这里没有在使用小括号，因为我们并不是在调用hi函数
# # 而是在将它放在greet变量里头。我们尝试运行下这个
# print(greet())
# # 如果我们删掉旧的hi函数，看看会发生什么！
# del hi
# print(greet())


#在函数中定义函数
# def hi(name="yasoob"):
#     print("now you are inside the hi() function")
#
#     def greet():
#         return "now you are in the greet() function"
#
#     def welcome():
#         return "now you are in the welcome() function"
#
#     print(greet())
#     print(welcome())
#     print("now you are back in the hi() function")
#
# hi()
# output:now you are inside the hi() function
#       now you are in the greet() function
#       now you are in the welcome() function
#       now you are back in the hi() function
# 上面展示了无论何时你调用hi(), greet()和welcome()将会同时被调用。
# 然后greet()和welcome()函数在hi()函数之外是不能访问的，比如：


#在函数中可以定义另外的函数。我们可以创建嵌套的函数。
# 函数也能返回函数
# def hi(name="yasoob"):
#     def greet():
#         return "now you are in the greet() function"
#
#     def welcome():
#         return "now you are in the welcome() function"
# #在 if/else 语句中我们返回 greet 和 welcome，而不是 greet() 和 welcome()。
# # 为什么那样？这是因为当你把一对小括号放在后面，这个函数就会执行；
# # 然而如果你不放括号在它后面，那它可以被到处传递，并且可以赋值给别的变量而不去执行它
#     if name == "yasoob":
#         return greet
#     else:
#         return welcome

#a = hi()
# print(a)
# # outputs: <function greet at 0x7f2143c01500>
# # 上面清晰地展示了`a`现在指向到hi()函数中的greet()函数
# # 现在试试这个
# print(a())
# b = hi(name='as')
# print(b())
# print(hi()())

#把函数作为参数传给另一个函数
# def hi():
#     return "hi yasoob!"
# def doSomethingBeforeHi(func):
#     print("I am doing some boring work before executing hi()")
#     print(func())
# doSomethingBeforeHi(hi)


#装饰器让你在一个函数的前后去执行代码
# def a_new_decorator(a_func):
#     def wrapTheFunction():
#         print("I am doing some boring work before executing a_func()")
#         a_func()
#         print("I am doing some boring work after executing a_func()")
#     return wrapTheFunction
# # python 中装饰器封装一个函数，并且用这样或者那样的方式来修改它的行为
# @a_new_decorator
# def a_function_requiring_decoration():
#     print("I am the function which needs some decoration to remove my foul smell")
# a_function_requiring_decoration()

#完整写法
# def a_new_decorator(a_func):
#     @wraps(a_func)
#     def wrapTheFunction():
#         print("I am doing some boring work before executing a_func()")
#         a_func()
#         print("I am doing some boring work after executing a_func()")
#
#     return wrapTheFunction
# @a_new_decorator
# def a_function_requiring_decoration():
#     print("I am the function which needs some decoration to ""remove my foul smell")
# print(a_function_requiring_decoration.__name__)


#一些装饰器实例
#蓝本规范
from urllib import request
# def decorator_name(f):
#     #接受一个函数来进行装饰，并加入了复制函数名称、注释文档、参数列表等等的功能。
#     # 这可以让我们在装饰器里面访问在装饰之前的函数的属性
#     @wraps(f)
#     def decorated(*args, **kwargs):
#         if not can_run:
#             return "Function will not run"
#         return f(*args, **kwargs)
#
#     return decorated
# @decorator_name
# def func():
#     return ("Function is running")
# can_run = True
# print(func())
# can_run = False
# print(func())

#日志
# def logit(func):
#     @wraps(func)
#     def with_logging(*args, **kwargs):
#         print(func.__name__ + " was called")
#         return func(*args, **kwargs)
#
#     return with_logging
# @logit
# def addition_func(x):
#     """Do some math."""
#     return x + x
# result = addition_func(4)

#日志生成  在函数中嵌入装饰器
# def logit(logfile='out.log'):
#     def logging_decorator(func):
#         @wraps(func)
#         def wrapped_function(*args, **kwargs):
#             log_string = func.__name__ + " was called"
#             print(log_string)
#             # 打开logfile，并写入内容
#             with open(logfile, 'a') as opened_file:
#                 # 现在将日志打到指定的logfile
#                 opened_file.write(log_string + '\n')
#             return func(*args, **kwargs)
#
#         return wrapped_function
#
#     return logging_decorator
# @logit()
# def myfunc1():
#     pass
# myfunc1()
# # 现在一个叫做 out.log 的文件出现了，里面的内容就是上面的字符串
# @logit(logfile='func2.log')
# def myfunc2():
#     pass
# myfunc2()


#装饰器类
class logit(object):
    def __init__(self, logfile='out.log'):
        self.logfile = logfile

    def __call__(self, func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            log_string = func.__name__ + " was called"
            print(log_string)
            # 打开logfile并写入
            with open(self.logfile, 'a') as opened_file:
                # 现在将日志打到指定的文件
                opened_file.write(log_string + '\n')
            # 现在，发送一个通知
            self.notify()
            return func(*args, **kwargs)

        return wrapped_function

    def notify(self):
        # logit只打日志，不做别的
        pass


@logit()
def myfunc1():
    pass
myfunc1()

#给 logit 创建子类，来添加 email 的功能(虽然 email 这个话题不会在这里展开
class email_logit(logit):
    '''
    一个logit的实现版本，可以在函数调用时发送email给管理员
    '''

    def __init__(self, email='admin@myproject.com', *args, **kwargs):
        self.email = email
        super(email_logit, self).__init__(*args, **kwargs)

    def notify(self):
        # 发送一封email到self.email
        # 这里就不做实现了
        pass
