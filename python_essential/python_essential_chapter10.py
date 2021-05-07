#-*- coding:utf-8 -*-
#Author:李明洲
#@Time:2020/4/2 17:32

#读取文件的一系列操作
# filename = 'pi_digits.txt'
# with open(filename) as file_object:
#     #创建一个包含文件各行内容的列表
#     lines = file_object.readlines()
#
# pi_string = ''
# for line in lines:
#     pi_string += line.rstrip()
# print(pi_string)
# print(len(pi_string))
# print('313'in pi_string)

#写入文件的一系列操作
#打开文件时 可指定读取模式（'r') 写入模式('w') 附加模式（'a') 读写模式('r+')  默认为只读模式
# filename = 'programming.txt'
# with open(filename, 'a') as file_object:
#     file_object.write("I love programming.\n")
#     file_object.write("I love creating new games.\n")

#异常处理
# try:
#     print(5/0)
# except ZeroDivisionError:
#     print("You can't divide by zero!")

#带异常处理的计算器 处理 ZeroDivisionError
# print("Give me two numbers, and I'll divide them.")
# print("Enter 'q' to quit.")
# while True:
#     first_number = input("\nFirst number: ")
#     if first_number == 'q':
#         break
#     second_number = input("Second number: ")
#     if second_number == 'q':
#         break
#
#     try:
#         answer = int(first_number) / int(second_number)
#     except ZeroDivisionError:
#          print("You can't divide by 0!")
#     else:
#         print(answer)

#处理FileNotFoundError
# filename = 'alice.txt'
# try:
#     with open(filename) as f_obj:
#         contents = f_obj.read()
# except FileNotFoundError:
#     msg = "Sorry, the file " + filename + " does not exist."
#     print(msg)

#处理多个文件
# def count_words(filename):
#     """计算一个文件包含多少个单词"""
#     try:
#         with open(filename) as f_obj:
#             contents = f_obj.read()
#     except FileNotFoundError:
#         # msg = "Sorry, the file " + filename + " does not exist."
#         # print(msg)
#         pass #在失败时一声不吭
#     else:
#     # 计算文件包含多少个单词
#         words = contents.split()
#         num_words = len(words)
#         print("The file " + filename + " has about " + str(num_words) +" words.")
# filename = 'alice.txt'
# count_words(filename)

#存储文件 使用json
import json
# #numbers = [2, 3, 5, 7, 11, 13]
# filename = 'numbers.json'
# # with open(filename, 'w') as f_obj:
# #     json.dump(numbers, f_obj)
# with open(filename) as f_obj:
#     numbers = json.load(f_obj)
# print(numbers)

#username = input("What is your name? ")
#filename = 'username.json'
# with open(filename, 'w') as f_obj:
#     json.dump(username, f_obj)
#     print("We'll remember you when you come back, " + username + "!")
# filename = 'username.json'
# with open(filename) as f_obj:
#     username = json.load(f_obj)
#     print("Welcome back, " + username + "!")

#添加异常处理 不存在则创建文件 存在则读取
# filename = 'username.json'
# try:
#     with open(filename) as f_obj:
#         username = json.load(f_obj)
# except FileNotFoundError:
#     username = input("What is your name? ")
#     with open(filename, 'w') as f_obj:
#         json.dump(username, f_obj)
#         print("We'll remember you when you come back, " + username + "!")
# else:
#     print("Welcome back, " + username + "!")

#重构化的操作

import json
def get_stored_username():
    """如果存储了用户名就获取"""
    filename = 'username.json'
    try:
        with open(filename) as f_obj:
            username = json.load(f_obj)
    except FileNotFoundError:
        return None
    else:
        return username

def get_new_username():
    """提示用户输入用户名"""
    username = input("What is your name? ")
    filename = 'username.json'
    with open(filename, 'w') as f_obj:
        json.dump(username, f_obj)
    return username

def greet_user():
    """问候用户并指出其名字"""
    username = get_stored_username()
    if username:
        print("Welcome back, " + username + "!")
    else:
        username=get_new_username()
        print("We'll remember you when you come back, " + username + "!")
greet_user()