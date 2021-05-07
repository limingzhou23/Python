#-*- coding:utf-8 -*-
#Author:李明洲
#python 3.7.3
#@Time:2020/6/9 12:40

import random

#线性结构：队列的实现和一些应用
#FIFO
class Queue:
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    #复杂度为O(n),因为删除列表首个元素列表需要重排列
    def enqueue(self,item):
        self.items.insert(0,item)

    #输出队首元素
    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

#应用一：热土豆问题
#参数：传土豆的名单，每次隔几个输出
#最后只剩一个人

# def hotPotato(namelist,num):
#     simqueue=Queue()
#     outname=[]
#     for name in namelist:
#         simqueue.enqueue(name)
#
#     while simqueue.size() > 1:
#         for i in range(num):
#             simqueue.enqueue(simqueue.dequeue())
#         outname.append(simqueue.dequeue())
#
#     return outname,simqueue.dequeue()
#print(hotPotato(["Bill","David","Susan","Jane","Kent","Brad"],7))

#应用二：打印模型
class Printer:
    def __init__(self,ppm):
        self.pagerate = ppm #打印速度
        self.currentTask=None #打印任务
        self.timeRemaining = 0 #任务倒计时

    def tick(self):
        if self.currentTask != None:
            self.timeRemaining = self.timeRemaining - 1
            if self.timeRemaining <= 0:
                self.currentTask = None

    def busy(self):
        if self.currentTask != None:
            return True
        else:
            return False

    def startNext(self,newtask):
        self.currentTask = newtask
        self.timeRemaining = (newtask.getPages()*60)/self.pagerate

class Task:
    def __init__(self,time):
        self.timestamp = time  #生成时间戳
        self.pages = random.randrange(1,21)

    def getStamp(self):
        return self.timestamp

    def getPages(self):
        return self.pages

    def waitTime(self,currenttime):
        return currenttime - self.timestamp

def newPrintTask():
    num=random.randrange(1,181)
    if num == 180:
        return True
    else:
        return False

def simulation(numSeconds,pagesPerMinute):
    labprinter = Printer(pagesPerMinute)
    printQueue = Queue()
    waitingtimes = []

    for currentSecond in range(numSeconds):
        if newPrintTask():
            task = Task(currentSecond)
            printQueue.enqueue(task)

        if (not labprinter.busy()) and (not printQueue.isEmpty()):
            nexttask=printQueue.dequeue()
            waitingtimes.append(nexttask.waitTime(currentSecond))
            labprinter.startNext(nexttask)

        labprinter.tick()

    averageWait = sum(waitingtimes)/len(waitingtimes)
    print("Average wait %6.2f secs %3d tasks remaining."%(averageWait,printQueue.size()))

for i in range(10):
    simulation(3600,20)


#双端队列的实现及其应用
class Deque:
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    def addFront(self,item):
        self.items.append(item)

    def addRear(self,item):
        self.items.insert(0,item)

    def removeFront(self):
        return self.items.pop()

    def removeRear(self):
        return self.items.pop(0)

    def size(self):
        return len(self.items)


#应用：回文词判定
def plachecker(aString):
    chardeque=Deque()

    for ch in aString:
        chardeque.addRear(ch)

    stillEqual = True

    while chardeque.size()>1 and stillEqual:
        first = chardeque.removeFront()
        last = chardeque.removeRear()
        if first != last:
            stillEqual = False

    return stillEqual

print(plachecker('lsdkjfskf'))
print(plachecker("radar"))
