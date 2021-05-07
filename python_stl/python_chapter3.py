'''
-*- coding: utf-8 -*-
@Author  : 李明洲
@Time    : 2020/7/27 14:50
@Software: PyCharm
@File    : python_object1.py
'''

#对内建容器的灵活使用

import sys
from functools import total_ordering
from heapq import heappush, heappop
from functools import reduce
from collections import defaultdict

# users = [{
#     'name': 'jim',
#     'sex': 'MALE',
#     'city': 'guangzhou',
#     'score': 28000,
# }, {
#     'name': 'lily',
#     'sex': 'FEMALE',
#     'city': 'shenzhen',
#     'score': 25000,
# }, ...]

#传统的列表比较方法
# 数据项类型 dict 不是 可比较 的，
# 我们需要提供 key 函数，为每个数据项生成可比较的排序 key 。
# 排序 key 可以是单字段，也可以复合字段，还可以是数据项的某种运算结果
# users.sort(key=lambda user: user['score'])


#借助 total_ordering 装饰器
#我们只需要实现 __eq__ 和 __lt__ 两个比较方法，
#其他诸如 __gt__ 等均由 total_ordering 自动生成
@total_ordering
class User:
    def __init__(self, name, sex, city, score):
        self.name = name
        self.sex = sex
        self.city = city
        self.score = score

    def __eq__(self, other):
        return self.score == other.score

    def __lt__(self, other):
        return self.score < other.score

users = [
    User(name='jim', sex='MALE', city='guangzhou', score=28000),

    User(name='lily', sex='FEMALE', city='shenzhen', score=25000),
]

#users.sort()

#制作排行榜
#排名前 100 位的用户制作成积分榜，可以先按积分 降序排序 ，再取出前 100 个
#方法一 排序 时间复杂度O(NlogN)
# users.sort(reverse=True)
# top100 = users[:100]

#方法二 最小堆 不需要全局排序
# 用一个最小堆来保存积分最多的100位用户
# 这样，积分最小的用户刚好在堆顶
# top100 = []
# # 遍历所有用户
# for user in users:
#     # 堆未满100，不断压入
#     if len(top100) < 100:
#         heappush(top100, user)
#         continue
#
#     # 如果当前用户积分比堆中积分最少的用户多，则替换
#     if user > top100[0]:
#         heappop(top100)
#         heappush(top100, user)
# 遍历完毕后，现在堆中保存的用户就是积分最多的100位了


#列表推导
#假设我们需要将一个用 dict 表示的用户信息列表转换成一个用 User 类表示的新列表
# new_users = []
# for user in users:
#     new_users.append(User(**user))
#思路平白无奇，但我们可以用 列表推导 语法对代码进行优化：
#new_users = [User(**user) for user in users]

#列表过滤
#列表推导支持通过 if 关键字过滤部分符合指定条件的数据项
# new_users = [
#     user
#     for user in users
#     if user.sex == 'MALE'
# ]

#可以借助 filter 内建函数进行过滤
#filter 函数接收两个参数：
#function ，判定函数，以数据项为参数，返回过滤结果，True 或者 False；
#iterable ，可迭代对象
new_users = list(
    filter(lambda user: user.sex == 'MALE', users),
)

#抽象运算
nums = list(range(1,10))
#filter 函数将符合条件的对象从对象集中过滤出来，条件以 判定函数 的形式指定。
# 判定函数以具体对象为参数，返回一个真值，以表明该对象是否符合条件
list(filter(lambda x: x%2 == 0, nums))
#map 函数对集合中每个对象进行加工，将其映射成一个新对象。
# 加工方法由操作函数指定，操作函数以待加工对象为参数，计算并返回加工结果
list(map(lambda x: x**2, nums))
#reduce 函数对多个对象进行汇总，并生成结果。
# 汇聚方法由操作函数指定，操作函数接收两个对象，并计算并返回这两个对象的合并结果。
# 例如，对所有整数进行求和
reduce(lambda x, y: x+y, nums)

#字典默认值
city2users = {}
#setdefault 方法先检查给定键是否已存在，未存在则以第二个参数进行初始化，
# 最后返回与键关联的值
for user in users:
    city = user.city
    city2users.setdefault(city, []).append(user)


#defaultdict
#标准库 collections 模块中的 defaultdict ，完美解决字典默认值问题。
# defaultdict 接收一个 default_factory 参数，当访问到不存在的键时，defaultdict 调用 default_factory 为其提供一个初始值。
city2users = defaultdict(list)

for user in users:
    city2users[user.city].append(user)

#树结构 借助 defaultdict ，我们只需一行代码便可实现一个树形存储容器：
Tree = lambda: defaultdict(Tree)
#这行代码的巧妙之处在于 递归 。Tree 函数初始化一棵树，树的实际结构是一个 defaultdict 对象。
# 当我们访问一个不存在的树分支时，defaultdict 再次调用 Tree 函数完成子树的初始化

# 初始化一棵树
tree = Tree()

# 存入一些数据
tree['fruits']['apple'] = 10
tree['fruits']['pear'] = 20
tree['pets']['cat'] = 3
tree['pets']['dog'] = 1

# 树的第一层有两个分支
print(tree.keys())

# fruits子树下有两个节点
print(tree['fruits'].items())


#数量统计是一个非常常见的场景，为此 collections 模块提供了一个更趁手的解决方案—— Counter 类：

from collections import Counter

city2total = Counter()

for user in users:
    city2total[user.city] += 1