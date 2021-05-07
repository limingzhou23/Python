#-*- coding:utf-8 -*-
#Author:李明洲
#python3.7.3
#@Time:2020/4/13 11:04

from urllib.parse import parse_qs
import os

my_values=parse_qs('red=5&blue=0&green=',keep_blank_values=True)

red=my_values.get('red',[''])[0] or 0
green=my_values.get('green',[''])[0] or 0
opacity=my_values.get('opacity',[''])[0] or 0
print('red:    %r'%red)
print('Green:    %r'%green)
print('Opacity:    %r'%opacity)