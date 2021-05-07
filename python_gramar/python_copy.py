#-*- coding:utf-8 -*-
#Author:李明洲
#python 3.7.3
#@Time:2020/6/9 20:09

#python赋值 深拷贝 浅拷贝机制
#Python3中，有6个标准的数据类型，他们又分为可以变和不可变。
#不可变：Number（数字）、String（字符串）、Tuple（元组）。
#可以变：List（列表）、Dictionary（字典）、Set（集合）。

import copy

#原始对象
l1 = [1, 2, 3, 4, 5, ['a', 'b']]

#赋值，传对象的引用
l2 = l1
#对象拷贝，浅拷贝
l3 = l1.copy()
#对象拷贝，深拷贝
l4 = copy.deepcopy(l1)

#id() 函数返回对象的唯一标识符，标识符是一个整数。
print("l1 = ",l1,"    id(l1) = ",id(l1),"id(l1[5]) = ",id(l1[5]))
print("l2 = ",l2,"    id(l2) = ",id(l2),"id(l2[5]) = ",id(l2[5]))
print("l3 = ",l3,"    id(l3) = ",id(l3),"id(l3[5]) = ",id(l3[5]))
print("l4 = ",l4,"    id(l4) = ",id(l4),"id(l4[5]) = ",id(l4[5]))
print("*"*70)

l1.append(6)
l1[5].append('c')
print("l1 = ",l1,"    id(l1) = ",id(l1),"id(l1[5]) = ",id(l1[5]))
print("l2 = ",l2,"    id(l2) = ",id(l2),"id(l2[5]) = ",id(l2[5]))
print("l3 = ",l3,"    id(l3) = ",id(l3),"id(l3[5]) = ",id(l3[5]))
print("l4 = ",l4,"    id(l4) = ",id(l4),"id(l4[5]) = ",id(l4[5]))

#可以看出深拷贝和浅拷贝的区别
#浅拷贝指在赋值过程中，只复制一层变量，不会复制深层变量绑定的对象的复制过程
#复制对象及关联的对象一起复制的过程叫深拷贝