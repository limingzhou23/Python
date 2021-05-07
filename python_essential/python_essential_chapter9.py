#-*- coding:utf-8 -*-
#Author:李明洲
#@Time:2020/4/2 16:50

# class Dog():
#     """模拟小狗的简单尝试"""
#     def __init__(self, name, age):
#         """属性name和age"""
#         self.name = name
#         self.age = age
#     def sit(self):
#         """蹲下"""
#         print(self.name.title() + " is now sitting.")
#     def roll_over(self):
#         """叫喊"""
#         print(self.name.title() + " rolled over!")
#
# my_dog = Dog('willie', 6)
# print("My dog's name is " + my_dog.name.title() + ".")
# print("My dog is " + str(my_dog.age) + " years old.")

#类的一些使用实例
class Car():
    """一次模拟车的简单尝试"""

    def __init__(self, make, model, year):
        """"类的初始化"""
        self.make = make
        self.model = model
        self.year = year
        self.odometer_reading=0

    def get_descriptive_name(self):
        """返回整体的描述性信息"""
        long_name = str(self.year) + ' ' + self.make + ' ' + self.model
        return long_name.title()

    def read_odometer(self):
        """打印一条指出里程的消息"""
        print("This car has " + str(self.odometer_reading) + " miles on it.")

    def update_odometer(self, mileage):
        """将里程表读数设置为指定的值"""
        if mileage >= self.odometer_reading:
            self.odometer_reading = mileage
        else:
            print("You can't roll back an odometer!")

    def increment_odometer(self, miles):
        """将里程表读数增加指定的量"""
        self.odometer_reading += miles

#将实例用作属性
class Battery():
    """"模拟电动车电瓶"""
    def __init__(self,battery_size=70):
        """"描述电瓶的属性"""
        self.battery_size=battery_size

    def describe_battery(self):
        """打印一条描述电池容量的消息"""
        print("This car has a " + str(self.battery_size) + "-kWh battery.")

    def get_range(self):
        """打印一条消息,指出电瓶的续航里程"""
        if self.battery_size == 70:
            dis_range = 240
        elif self.battery_size == 85:
            dis_range = 270
        message = "This car can go approximately " + str(dis_range)
        message += " miles on a full charge."
        print(message)

#创建子类 将大型类合并为多个小类的分工 每个处理一部分细节
class ElectricCar(Car):
    """电动车的独特之处"""
    def __init__(self, make, model, year):
        """初始化父类的属性"""
        super().__init__(make, model, year)
        self.battery=Battery()


my_tesla = ElectricCar('tesla', 'model s', 2016)
print(my_tesla.get_descriptive_name())
my_tesla.battery.describe_battery()
my_tesla.battery.get_range()
