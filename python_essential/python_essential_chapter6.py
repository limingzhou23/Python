#-*- coding:utf-8 -*-
#Author:李明洲
#@Time:2020/4/2 14:20

#python入门到实践  第六章

# alien_0 = {'color': 'green', 'points': 5}
# new_points = alien_0['points']
# print("You just earned " + str(new_points) + " points!")
# alien_0['x_position'] = 0
# alien_0['y_position'] = 25
# print(alien_0)

# alien_0 = {'x_position': 0, 'y_position': 25, 'speed': 'medium'}
# print("Original x-position: " + str(alien_0['x_position']))
# # 向右移动外星人
# # 据外星人当前速度决定将其移动多􁇺
# if alien_0['speed'] == 'slow':
#     x_increment = 1
# elif alien_0['speed'] == 'medium':
#     x_increment = 2
# else:
#     x_increment = 3
# # 新位置等于􀪻位置加上增量
# alien_0['x_position'] = alien_0['x_position'] + x_increment
# print("New x-position: " + str(alien_0['x_position']))

# alien_0 = {'color': 'green', 'points': 5}
# print(alien_0)
# del alien_0['points']
# print(alien_0)

# user_0 = {
# 'username': 'efermi',
# 'first': 'enrico',
# 'last': 'fermi',
# }
# #字典(Dictionary) items() 函数以列表返回可遍历的(键, 值) 元组数组
# for key, value in user_0.items():
#     print("\nKey: " + key)
#     print("Value: " + value)

# favorite_languages = {
# 'jen': 'python',
# 'sarah': 'c',
# 'edward': 'ruby',
# 'phil': 'python',
# }
# #遍历所有键
# for name in favorite_languages.keys():
#     print(name.title())

# favorite_languages = {
# 'jen': 'python',
# 'sarah': 'c',
# 'edward': 'ruby',
# 'phil': 'python',
# }
# friends = ['phil', 'sarah']
# for name in favorite_languages.keys():
#     print(name.title())
#     if name in friends:
#         print(" Hi " + name.title() +", I see your favorite language is " +favorite_languages[name].title() + "!")

#按顺序遍历字典
# favorite_languages = {
# 'jen': 'python',
# 'sarah': 'c',
# 'edward': 'ruby',
# 'phil': 'python',
# }
# for name in sorted(favorite_languages.keys()):
#     print(name.title() + ", thank you for taking the poll.")

#遍历所有的值 并用set剔除重复
# favorite_languages = {
# 'jen': 'python',
# 'sarah': 'c',
# 'edward': 'ruby',
# 'phil': 'python',
# }
# print("The following languages have been mentioned:")
# for language in set(favorite_languages.values()):
#     print(language.title())

# #字典的嵌套
# # 创建一个用于存储外星人的空列表
# aliens = []
# # 创建30个􀭴色的外星人
# for alien_number in range(30):
#     new_alien = {'color': 'green', 'points': str(alien_number), 'speed': 'slow'}
#     aliens.append(new_alien)
# # 显示前􀿵个外星人
# for alien in aliens[:5]:
#     print(alien)
# print("...")
# # 显示创建了多少个外星人
# print("Total number of aliens: " + str(len(aliens)))

#储存列表的字典
# pizza = {
# 'crust': 'thick',
# 'toppings': ['mushrooms', 'extra cheese'],
# }
# # 概述所点的比􀷔
# print("You ordered a " + pizza['crust'] + "-crust pizza " +
# "with the following toppings:")
# for topping in pizza['toppings']:
#     print("\t" + topping)

#字典中储存字典
users = {
'aeinstein': {
'first': 'albert',
'last': 'einstein',
'location': 'princeton',
},
'mcurie': {
'first': 'marie',
'last': 'curie',
'location': 'paris',
},
}
for username, user_info in users.items():
    print("\nUsername: " + username)
    full_name = user_info['first'] + " " + user_info['last']
    location = user_info['location']
    print("\tFull name: " + full_name.title())
    print("\tLocation: " + location.title())