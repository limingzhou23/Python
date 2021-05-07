#-*- coding:utf-8 -*-
#Author:李明洲
#@Time:2020/6/3 20:57

#数据结构堆的创建及操作
#此处为最大堆

import numpy as np

class HNode:
    def __init__(self,capacity=0):
        self.Data = []
        self.Size = 0
        self.Capacity = capacity
        self.Maxdata = 1000

def CreatHeap(maxsize):
    MaxHeap = HNode(maxsize)
    MaxHeap.Data.append(MaxHeap.Maxdata)
    return MaxHeap

def IsFull(MaxHeap):
    return MaxHeap.Size == MaxHeap.Capacity

def IsEmpty(MaxHeap):
    return MaxHeap.Size == 0

#插入操作
def Insert(MaxHeap,elem):
    if(IsFull(MaxHeap)):
        print("Heap is Full,can't insert "+str(elem))
        return False

    i = MaxHeap.Size+1
    MaxHeap.Size+=1
    MaxHeap.Data.insert(i+1,0)
    while(MaxHeap.Data[i//2]<elem):
        MaxHeap.Data[i]=MaxHeap.Data[i//2]
        i=i//2
    MaxHeap.Data[i]=elem
    return True


#删除操作
def DeleteMax(MaxHeap):
    parent=1
    if(IsEmpty(MaxHeap)):
        print("Heap is empty,can't delete")
        return False

    MaxItem=MaxHeap.Data[1]
    X=MaxHeap.Data[MaxHeap.Size]
    MaxHeap.Size-=1

    #当当前父节点存在孩子结点时继续循环 因为结点i的左孩子为2i
    #循环目的是找到插入的地方
    while(parent*2<=MaxHeap.Size):
        child=parent*2
        #chilid表示左孩子 第一个判断是存在右结点时 第二个判断是找出大的孩子
        if (child!=MaxHeap.Size)and(MaxHeap.Data[child]<MaxHeap.Data[child+1]):
            child+=1
        if(X>MaxHeap.Data[child]):
            break
        else:MaxHeap.Data[parent]=MaxHeap.Data[child]

        parent=child
    MaxHeap.Data[parent]=X
    return MaxItem

#建造最大堆
#下滤操作
def PercDown(MaxHeap,p):
    #将MaxHeap中以MaxHeap.Data[p]为根的子堆调整为最大堆
    parent=p
    X=MaxHeap.Data[p]
    while(parent*2 < MaxHeap.Size):
        child=parent*2
        if((child != MaxHeap.Size)and(MaxHeap.Data[child]<MaxHeap.Data[child+1])):
            child += 1
        if(X>=MaxHeap.Data[child]):break
        else:
            MaxHeap.Data[parent]=MaxHeap.Data[child]
        parent = child

    MaxHeap.Data[parent]=X
    return

def BuildHeap(MaxHeap):
    i=MaxHeap.Size//2
    for index in range(i,0):
        PercDown(MaxHeap,i)

if __name__ == '__main__':
    H=CreatHeap(10)

    for i in range(5,16):
        Insert(H,i)
    print(H.Data)
    BuildHeap(H)
    print(H.Data)
    print(DeleteMax(H))







