#-*- coding:utf-8 -*-
#Author:李明洲
#@Time:2020/4/2 16:07

# def greet_user():
#     #文档字符串
#     """显示简单的问􀢪语"""
#     print("Hello!")
#
# greet_user()

# #带默认参数的函数
# def describe_pet(pet_name,animal_type='dog' ):
#     """显示􀘎物的信息"""
#     print("\nI have a " + animal_type + ".")
#     print("My " + animal_type + "'s name is " + pet_name.title() + ".")
# # 位置实参
# describe_pet('harry','hamster')
# #关键字实参
# describe_pet(pet_name='harry')

#可选实参的函数
# def get_formatted_name(first_name, last_name,midle_name=''):
#     """返回全名"""
#     if midle_name:
#         full_name = first_name + ' ' + midle_name+' '+ last_name
#     else:
#         full_name=first_name+' '+last_name
#     return full_name.title()
# musician = get_formatted_name('jimi', 'hendrix')
# print(musician)
# musician_2= get_formatted_name('jimi', 'hendrix','lee')
# print(musician_2)

# def print_models(unprinted_designs, completed_models):
#     """
#     模拟打印每个设计􀇈直到􀯁有未打印的设计为止
#     打印每个设计后􀇈都将其移到列表completed_models中
#     """
#     while unprinted_designs:
#         current_design = unprinted_designs.pop()
#         # 模拟根据设计制作3D打印模型的过程
#         print("Printing model: " + current_design)
#         completed_models.append(current_design)
#
# def show_completed_models(completed_models):
#     """显示打印 的所有模型"""
#     print("\nThe following models have been printed:")
#     for completed_model in completed_models:
#         print(completed_model)
#
# unprinted_designs = ['iphone case', 'robot pendant', 'dodecahedron']
# completed_models = []
# print_models(unprinted_designs, completed_models)
# show_completed_models(completed_models)

#传递任意数量的实参
# def make_pizza(*toppings):
#     """顾客点的所有披萨"""
#     print("\nMaking a pizza with the following toppings:")
#     for topping in toppings:
#         print("- " + topping)
# make_pizza('pepperoni')
# make_pizza('mushrooms', 'green peppers', 'extra cheese')

#使用任意数量的关键字实参
def build_profile(first, last, **user_info):
    """创建一个字典􀇈其中包含􀿢􀯍􁊪􀚢的有关用户的一切"""
    profile = {}
    profile['first_name'] = first
    profile['last_name'] = last
    for key, value in user_info.items():
        profile[key] = value
    return profile
user_profile = build_profile('albert', 'einstein',location='princeton',field='physics')
print(user_profile)