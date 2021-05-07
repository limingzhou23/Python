#-*- coding:utf-8 -*-
#Author:李明洲
#python 3.7.3
#@Time:2020/6/9 20:21

import collections
# 字典是另一种可变容器模型，且可存储任意类型对象。
# 字典的每个键值 key=>value 对用冒号‘:’ 分割，每个键值对之间用逗号‘,’分割，
# 整个字典包括在花括号 {} 中 ,格式如下所示：
dict={'key1' : 'value1', 'key2' : 'value1'}
print(dict['key1'])       # 键是唯一的，值不是唯一的。


dict['key3'] = 8
print(dict)

del dict['key3']
print(dict)

#字典特性：
# 字典值可以没有限制地取任何python对象，既可以是标准的对象，也可以是用户定义的，但键不行。
#两个重要的点需要记住：
#1）不允许同一个键出现两次。创建时如果同一个键被赋值两次，后一个值会被记住
dict = {'Name': 'Zara', 'Age': 7, 'Name': 'Manni'}
print("dict['Name']: ", dict['Name'])
#2）键必须不可变，所以可以用数字，字符串或元组充当，所以用列表就不行，如下实例

#字典内置函数：
dict = {'Name': 'Runoob', 'Age': 7, 'Class': 'First'}
print(len(dict))

#str(dict) 输出字典，以可打印的字符串表示
print(str(dict))

#type(variable) 返回输入的变量类型，如果变量是字典就返回字典类型。
print(type(dict))

#clear() 删除所有元素
#copy() 返回一个字典的浅拷贝

#fromkeys()方法
seq = ('name', 'age', 'sex')
dict = dict.fromkeys(seq)
print("新的字典为 : %s" % str(dict))
dict = dict.fromkeys(seq, [10,11])
print("新的字典为 : %s" % str(dict))

#get()方法  可指定默认值
print ("Age 值为 : %s" %  dict.get('name'))
print ("Sex 值为 : %s" %  dict.get('Sex', "NA"))

#key in dict 键在字典dict里返回true，否则返回false

#items()方法
print(dict.items())
for k,v in dict.items():
    print(k,v)

#keys()方法 返回一个迭代器，可以使用 list() 来转换为列表
print(dict.keys())
print(list(dict.keys()))

#values() 返回一个迭代器，可以使用 list() 来转换为列表
print(dict.values())
print(list(dict.values()))

#setdefault()方法：和get()类似, 但如果键不存在于字典中，将会添加键并将值设为default
print ("Age 键的值为 : %s" %dict.setdefault('Age', None))
print ("Sex 键的值为 : %s" %dict.setdefault('Sex', None))
print ("新字典为：", dict)

#update(dict2) 把字典dict2的键/值对更新到dict里
dict1 = {'Name': 'Runoob', 'Age': 7}
dict2 = {'Sex': 'female'}
dict1.update(dict2)
print("更新字典 dict1 : ", dict1)

# pop(key[,default])
# 删除字典给定键 key 所对应的值，返回值为被删除的值。key值必须给出。
# 若要删除的key不存在，返回default值。
site = {'name': '菜鸟教程', 'alexa': 10000, 'als':222,'url': 'www.runoob.com'}
pop_obj = site.pop('name','a')
print(pop_obj)

# popitem()
# 随机返回并删除字典中的最后一对键和值。
print(site.popitem())
print(site)

#defaultdict使用
# 使用defaultdict任何未定义的key都会默认返回一个根据method_factory参数不同的默认值,
# 而相同情况下dict()会返回KeyError.
s = 'mississippi'
d = collections.defaultdict(int)
for k in s:
    d[k] += 1
print(d)

s = [('red', 1), ('blue', 2), ('red', 3), ('blue', 4), ('red', 1), ('blue', 4)]
d = collections.defaultdict(set)
for k, v in s:
    d[k].add(v)
print(d)

