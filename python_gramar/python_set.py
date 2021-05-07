'''
-*- coding: utf-8 -*-
@Author  : 李明洲
@Time    : 2020/6/27 10:52
@Software: PyCharm
@File    : python_set.py
'''

# set是一个无序不重复的序列
# 可以用 { } 或者 set( ) 函数创建集合
# 集合存放不可变类型（字符串、数字、元组）
# 注意：创建一个空集合必须用 set( ) 而不是 { } ，因为 { } 是用来创建一个空字典

# add(x)将元素x添加到集合里
# s = {1,2,3,4,5,}
# s.add('5')
# print(s)

# update(x),将x添加到集合中，且参数可以是列表、元组、字典等
# s = {'a', 'cc', 'f'}
# # 添加字典只能添加不可变的--键
# dict_1 = {'name': 'bb', 'age': 'cc', 'f': 11}
# s.update(dict_1)
# print("添加字典"+str(s))

# s = {'a', 'cc', 'f'}
# tup_1 = (1, 2,)
# s.update(tup_1)
# print(s)

# s = {'a', 'cc', 'f'}
# list_1 = ['w', 'a', 1]
# s.update(list_1)
# print(s)
# # 移除集合中元素，如果移除的元素不在集合中将发生错误
# s.remove('cc')
# print(s)
# # 移除集合中元素，如果移除的元素不在集合中不会发生错误
# s.discard('mm')
# print(s)
# # 随机删除集合中元素
# s.pop()
# print(s)
# print('集合元素个数为：'+str(len(s)))
# s1 = s.copy()#集合拷贝
# print(s1)
# s1.clear()  #集合清零
# print(s1)

# # difference求差集 或者用 -
# s = {'a', 'cc', 'f'}
# s1 = {'a', 'f', 1, 'ww'}
# # 两种求差集的方法
# print("在s中不在s1中: "+str(s.difference(s1)))
# print('在s1中不在s中： '+str(s1-s))
# # 除集合s和集合s1共有的以外的元素
# print(s.symmetric_difference(s1))
# print(s1^s)
#交集(&) 并集(|)同理

#一个集合是否是另一个集合的子集
# s = {'a', 'cc', 'f'}
# s1 = {'a', 'f'}
# print(s.issubset(s1))
# print(s1.issubset(s))

#一个集合是否是另一个的父集
s = {'a', 'cc', 'f'}
s1 = {'a', 'f'}
print(s.issuperset(s1))
print(s1.issuperset(s))
# s1是s的子集，s是s1的父集
print(s1.issubset(s))

#检测2个集合是否不存在交集 存在交集 False
s1 = {'ljl','wc','xy','zb','lsy'}
s2 = {'mmf','lsy','syj'}
s3 = {1, 2}
print(s1.isdisjoint(s2))
print(s1.isdisjoint(s3))