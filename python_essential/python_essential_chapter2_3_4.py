#-*- coding:utf-8 -*-
#Author:李明洲
#@Time:2020/4/2 15:02

#大小写变化 title() lower()
#删除空格  ltrip左边空格  rstrip末尾空格 strip两侧的空格

#列表添加元素
# motorcycles = ['honda', 'yamaha', 'suzuki']
# motorcycles.insert(0, 'ducati')
# print(motorcycles)

#列表删除元素
# motorcycles = ['honda', 'yamaha', 'suzuki']
# print(motorcycles)
# del motorcycles[0]
# print(motorcycles)

#弹出末尾元素 可以选择位置
# motorcycles = ['honda', 'yamaha', 'suzuki']
# print(motorcycles)
# popped_motorcycle = motorcycles.pop()
# print(motorcycles)
# print(popped_motorcycle)

#根据值删除元素
# motorcycles = ['honda', 'yamaha', 'suzuki', 'ducati']
# print(motorcycles)
# motorcycles.remove('ducati')
# print(motorcycles)

#临时排序
# cars = ['bmw', 'audi', 'toyota', 'subaru']
# print("Here is the original list:")
# print(cars)
# print("\nHere is the sorted list:")
# print(sorted(cars))
# print("\nHere is the original list again:")
# print(cars)

#列表颠倒
# cars = ['bmw', 'audi', 'toyota', 'subaru']
# print(cars)
# cars.reverse()
# print(cars)

#创建数值列表
# even_numbers = list(range(2,11,2))
# print(even_numbers)

#列表解析
squares = [value**2 for value in range(1,11)]
print(squares)

#列表的复制 注意浅拷贝和深拷贝