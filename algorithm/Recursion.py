#-*- coding:utf-8 -*-
#Author:李明洲
#python 3.7.3
#@Time:2020/6/10 13:53

import sys
import turtle

# #python递归深度有限制 必须设置基本结束条件
# #python 的sys 模块可以获得和调整最大递归深度
# #print(sys.getrecursionlimit())
#
# #数列求和
# def listsum(numList):
#     if len(numList) == 1:
#         return numList[0]
#     else:
#         return numList[0] + listsum(numList[1:])
# #print(listsum([1,2,3,4,5,4,3,2,1]))
#
# #进制转换
# def toStr(n,base=10):
#     covertString = "0123456789ABCDEF"
#     if n < base:
#         return covertString[n]
#     else:
#         return toStr(n//base,base) + covertString[n%base]
# #print(toStr(13423))
#
#
# #海龟作图
# #螺旋线
# def drawSpiral(t,linelen):
#     if linelen > 0:
#         t.forward(linelen)
#         t.right(90)
#         drawSpiral(t,linelen-5)
#
# #分形树：自相似递归图形
# #注意最后的回退操作
# def tree(t,branch_len):
#     print(branch_len)
#     if branch_len > 5:
#         t.forward(branch_len)
#         t.right(20)
#         tree(branch_len -15)
#         t.left(40)
#         tree(branch_len-15)
#         t.right(20)
#         t.backward(branch_len)
#
# # t=turtle.Turtle()
# # t.pencolor('red')
# # t.pensize(3)
# # t.speed(1)
# # t.left(90)
# # #抬起画笔
# # t.penup()
# # t.backward(100)
# # #放下画笔
# # t.pendown()
# # tree(t,30)
# # t.hideturtle()
# #
# # turtle.done()
#
# #谢尔宾斯基三角形
# # def drawTriangle(points,color):
# #     t.fillcolor(color)
# #     t.penup()
# #     t.goto(points['top'])
# #     t.pendown()
# #     t.begin_fill()
# #     t.goto(points['left'])
# #     t.goto(points['right'])
# #     t.goto(points['top'])
# #     t.end_fill()
#
# # def getMid(p1,p2):
# #     return ((p1[0] + p2[0]) / 2,(p1[1] + p2[1]) / 2)
# #
# # def sierpinski(degree,points):
# #     colormap = ['blue','red','green','white','yellow','orange']
# #     drawTriangle(points,colormap[degree])
# #     if degree > 0:
# #         sierpinski(degree-1,{'left':points['left'],
# #                              'top':getMid(points['left'],points['top']),
# #                               'right':getMid(points['left'],points['right'])})
# #         sierpinski(degree - 1, {'left': getMid(points['left'], points['top']),
# #                                 'top': points['top'],
# #                                 'right': getMid(points['top'], points['right'])})
# #         sierpinski(degree - 1, {'left': getMid(points['left'], points['right']),
# #                                 'top': getMid(points['top'], points['right']),
# #                                 'right': points['right']})
#
# # t=turtle.Turtle()
# # t.speed(1)
# # points = {'left':(-200,-100),
# #           'top':(0,200),
# #           'right':(200,-100)}
# # sierpinski(3,points)
# # turtle.done()
#
# #汉诺塔
# def moveDisk(disk,fromPole,toPole):
#     print(f"Moving disk[{disk}] from {fromPole} to {toPole}")
#
# def moveTower(height,fromPole,withPole,toPole):
#     if height >= 1:
#         moveTower(height-1,fromPole,toPole,withPole)
#         moveDisk(height,fromPole,toPole)
#         moveTower(height - 1,withPole,fromPole,toPole)
# #moveTower(3,'#1','#2','#3')
#
# #探索迷宫
# # 迷宫类
# # class Maze(object):
# # 	# 读取迷宫数据，初始化迷宫内部，并找到海龟初始位置。
# # 	def __init__(self, mazeFileName):
# # 		self.movenum=0
# # 		rowsInMaze = 0							#初始化迷宫行数
# # 		columnsInMaze = 0 						#初始化迷宫列数
# # 		self.mazelist = []						#初始化迷宫列表
# # 		mazeFile = open(mazeFileName, 'r')		#读取迷宫文件
# # 		for line in mazeFile:					#按行读取
# # 			rowList = [] 						#初始化行列表
# # 			col = 0 							#初始化列
# # 			# for ch in line[:-1]:				#这样会丢失最后一列
# # 			for ch in line:						#按列读取
# # 				rowList.append(ch)				#添加到行列表
# # 				if ch == 'S':					#S为乌龟初始位置，即迷宫起点
# # 					self.startRow = rowsInMaze	#乌龟初始行
# # 					self.startCol = col 		#乌龟初始列
# # 				col = col + 1 					#下一列
# # 			rowsInMaze = rowsInMaze + 1 		#下一行
# # 			self.mazelist.append(rowList)		#行列表添加到迷宫列表
# # 			columnsInMaze = len(rowList) 		#获取迷宫总列数
# # 		self.rowsInMaze = rowsInMaze 			#设置迷宫总行数
# # 		self.columnsInMaze = columnsInMaze		#设置迷宫总列数
# # 		self.xTranslate = -columnsInMaze/2 		#设置迷宫左上角的初始x坐标
# # 		self.yTranslate = rowsInMaze/2 			#设置迷宫左上角的初始y坐标
# # 		self.t = turtle.Turtle()				#创建一个海龟对象
# # 		self.t.shape('turtle')					#给当前指示点设置样式(类似鼠标箭头)，海龟形状为参数指定的形状名，指定的形状名应存在于TurtleScreen的shape字典中。多边形的形状初始时有以下几种："arrow", "turtle", "circle", "square", "triangle", "classic"。
# # 		self.wn = turtle.Screen()				#创建一个能在里面作图的窗口
# # 		self.wn.setworldcoordinates(-columnsInMaze/2, -rowsInMaze/2, columnsInMaze/2, rowsInMaze/2)			#设置世界坐标系，原点在迷宫正中心。参数依次为画布左下角x轴坐标、左下角y轴坐标、右上角x轴坐标、右上角y轴坐标
# #
# # 	# 在屏幕上绘制迷宫
# # 	def drawMaze(self):
# # 		self.t.speed(50)						#绘图速度
# # 		for y in range(self.rowsInMaze):		#按单元格依次循环迷宫
# # 			for x in range(self.columnsInMaze):
# # 				if self.mazelist[y][x] == OBSTACLE:	#如果迷宫列表的该位置为障碍物，则画方块
# # 					self.drawCenteredBox(x + self.xTranslate, -y + self.yTranslate, 'orange')
# #
# # 	# 画方块
# # 	def drawCenteredBox(self, x, y, color):
# # 		self.t.up()								#画笔抬起
# # 		self.t.goto(x - 0.5, y - 0.5)			#前往参数位置，此处0.5偏移量的作用是使乌龟的探索路线在单元格的正中心位置
# # 		self.t.color(color)						#方块边框为橙色
# # 		self.t.fillcolor('green')				#方块内填充绿色
# # 		self.t.setheading(90)					#设置海龟的朝向，标准模式：0 - 东，90 - 北，180 - 西，270 - 南。logo模式：0 - 北，90 - 东，180 - 南，270 - 西。
# # 		self.t.down()							#画笔落下
# # 		self.t.begin_fill()						#开始填充
# # 		for i in range(4):						#画方块边框
# # 			self.t.forward(1)					#前进1个单位
# # 			self.t.right(90)					#右转90度
# # 		self.t.end_fill()						#结束填充
# #
# # 	# 移动海龟
# # 	def moveTurtle(self, x, y):
# # 		self.movenum=self.movenum+1
# # 		self.t.up()								#画笔抬起
# # 		self.t.setheading(self.t.towards(x + self.xTranslate, -y + self.yTranslate))	#setheading()设置海龟朝向，towards()从海龟位置到由(x, y)，矢量或另一海龟位置连线的夹角。此数值依赖于海龟初始朝向，由"standard"、"world"或"logo" 模式设置所决定。
# # 		self.t.goto(x + self.xTranslate, -y + self.yTranslate)	#前往目标位置
# #
# # 	# 画路径圆点
# # 	def dropBreadcrumb(self, color):
# # 		self.t.dot(color)						#dot(size=None, color)画路径圆点
# #
# # 	# 用以更新迷宫内的状态及在窗口中改变海龟位置，行列参数为乌龟的初始坐标。
# # 	def updatePosition(self, row, col, val):
# # 		self.mazelist[row][col] = val 			#设置该标记状态为当前单元格的值
# # 		self.moveTurtle(col, row)				#移动海龟
# # 		if val == PART_OF_PATH: 				#其中一条成功路径的圆点的颜色
# # 			color = 'green'
# # 		elif val == TRIED:						#尝试用的圆点的颜色
# # 			color = 'black'
# # 		elif val == DEAD_END:					#死胡同用的圆点的颜色
# # 			color = 'red'
# # 		self.dropBreadcrumb(color)				#画路径圆点并上色
# #
# # 	# 用以判断当前位置是否为出口。
# # 	def isExit(self, row, col):
# # 		return (row == 0 or row == self.rowsInMaze - 1 or col == 0 or col == self.columnsInMaze - 1)								#根据海龟位置是否在迷宫的4个边线位置判断
# #
# # 	# 返回键对应的值，影响searchFrom()中maze[startRow][startColumn]值的获取
# # 	def __getitem__(self, key):
# # 		return self.mazelist[key]
# # # 探索迷宫，注意此函数包括三个参数：一个迷宫对象、起始行、起始列。
# # def searchFrom(maze, startRow, startColumn,num):
# # 	# 从初始位置开始尝试四个方向，直到找到出路。
# # 	# 1. 遇到障碍
# # 	print(num)
# # 	num=num+1
# # 	if maze[startRow][startColumn] == OBSTACLE:
# # 		return False
# # 	# 2. 发现已经探索过的路径或死胡同
# # 	if maze[startRow][startColumn] == TRIED or maze[startRow][startColumn]== DEAD_END:
# # 		return False
# # 	# 3. 发现出口
# # 	if maze.isExit(startRow, startColumn):
# # 		maze.updatePosition(startRow, startColumn, PART_OF_PATH)#显示出口位置，注释则不显示此点
# # 		return True
# # 	maze.updatePosition(startRow, startColumn, TRIED)#更新迷宫状态、设置海龟初始位置并开始尝试
# # 	# 4. 依次尝试每个方向
# # 	found = searchFrom(maze, startRow - 1, startColumn,num) or \
# #             searchFrom(maze, startRow + 1, startColumn,num) or \
# #             searchFrom(maze, startRow, startColumn - 1,num) or \
# #             searchFrom(maze, startRow, startColumn + 1,num)
# # 	if found:													#找到出口
# # 		maze.updatePosition(startRow, startColumn, PART_OF_PATH)#返回其中一条正确路径
# # 	else:														#4个方向均是死胡同
# # 		maze.updatePosition(startRow, startColumn, DEAD_END)
# # 	return found
# #
# # if __name__ == '__main__':
# # 	PART_OF_PATH = 'O'			#部分路径
# # 	TRIED = '.'					#尝试
# # 	OBSTACLE = '+'				#障碍
# # 	DEAD_END = '-'				#死胡同
# # 	num = 0
# #
# # 	myMaze = Maze('maze.txt')	#实例化迷宫类，maze文件是使用“+”字符作为墙壁围出空心正方形空间，并用字母“S”来表示起始位置的迷宫文本文件。
# # 	myMaze.drawMaze()			#在屏幕上绘制迷宫。
# # 	searchFrom(myMaze, myMaze.startRow, myMaze.startCol,num)	#探索迷宫
# # 	print(myMaze.movenum)
#
#
def reMC(coinValueList,change,knowResult):
    minCoins = change
    if change in coinValueList:
        knowResult[change] = 1
        return 1
    elif knowResult[change] > 0:
        return knowResult[change]
    else:
        for i in [c for c in coinValueList if c <= change]:
            numCoins = 1 + reMC(coinValueList,change - i,knowResult)
            if numCoins < minCoins:
                minCoins = numCoins
                knowResult[change] = minCoins
    return minCoins

coinValueList = [1,5,10,25]
change = 63
knownResult = [0]*(change+1)
list=[c for c in coinValueList if c <= change]
a = reMC(coinValueList,change,knownResult)
print(a)
