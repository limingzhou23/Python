'''
-*- coding: utf-8 -*-
@Author  : 李明洲
@Time    : 2020/6/26 20:45
@Software: PyCharm
@File    : python_reflact.py
'''

#反射机制
# 通过字符串的形式导入模块
# 通过字符串的形式，去模块中寻找指定的函数，并执行
# 规定用户输入格式 模块名/函数名 通过__import__的形式导入模块，并通过 hasattr和getattr 检查并获取函数返回值。
# 相关方法：
# 　　getattr:--根据字符串的形式去某个模块中寻找东西
# 　　hasattr:--根据字符串的形式去某个模块中判断东西是否存在
# 　　setattr:--根据字符串的形式去某个模块中设置东西
# 　　delattr:--根据字符串的形式去某个模块中删除东西

#1.根据字符串的形式导入模块。
#2.根据字符串的形式去对象（某个模块）中操作其成员　
#实现思路：规定用户输入格式 模块名/函数名
# 通过__import__的形式导入模块，并通过 hasattr和getattr 检查并获取函数返回值。
#通过用户输入的形式， 导入模块
inp = input('请输入需要导入的模块名：')
#__import__用于以字符串的形式导入模块
dd = __import__(inp)
#在模块寻找函数并执行函数
inp_func = input('请输入需要执行的函数：')
#getattr()用于以字符串的形式去某个模块中寻找函数
tager_func = getattr(dd, inp_func)
#在获取到的函数名后加()表示执行函数
result = tager_func()
print(result)