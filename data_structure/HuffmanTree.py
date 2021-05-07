#-*- coding:utf-8 -*-
#Author:李明洲
#@Time:2020/6/4 13:56

import heapq

#哈夫曼树 没有实现成功

class HNode:
    def __init__(self,value=0,capacity=0):
        self.value = value
        self.left=None
        self.right=None
        self.isIn=False

class MinHeap:
    def __init__(self,capacity=0):
        self.Maxdata=1000
        self.Capacity = capacity
        self.Size = 0
        self.root=HNode()
        self.Data=[]


#最小堆
def CreatHeap(maxsize):
    H = MinHeap(maxsize)
    H.root.value=H.Maxdata
    H.Data.append(H.Maxdata)
    return MinHeap

def IsFull(MinHeap):
    return MinHeap.Size == MinHeap.Capacity

def IsEmpty(MinHeap):
    return MinHeap.Size == 0

#插入操作
def Insert(MinHeap,elem):
    if(IsFull(MinHeap)):
        print("Heap is Full,can't insert "+str(elem))
        return False

    i = MinHeap.Size+1
    MinHeap.Size+=1
    MinHeap.Data.insert(i+1,0)
    while(MinHeap.Data[i//2]>elem):
        MinHeap.Data[i]=MinHeap.Data[i//2]
        i=i//2
    MinHeap.Data[i]=elem

#删除操作
def DeleteMin(MinHeap):
    parent=1
    if(IsEmpty(MinHeap)):
        print("Heap is empty,can't delete")
        return False

    MinItem=MinHeap.Data[1]
    X=MinHeap.Data[MinHeap.Size]
    MinHeap.Size-=1

    #当当前父节点存在孩子结点时继续循环 因为结点i的左孩子为2i
    #循环目的是找到插入的地方
    while(parent*2<=MinHeap.Size):
        child=parent*2
        #chilid表示左孩子 第一个判断是存在右结点时 第二个判断是找出小的孩子
        if (child!=MinHeap.Size)and(MinHeap.Data[child]>MinHeap.Data[child+1]):
            child+=1
        if(X<MinHeap.Data[child]):
            break
        else:MinHeap.Data[parent]=MinHeap.Data[child]

        parent=child
    MinHeap.Data[parent]=X
    return MinItem

#建造最小堆
#下滤操作
def PercDown(MinHeap,p):
    #将MaxHeap中以MaxHeap.Data[p]为根的子堆调整为最大堆
    parent=p
    X=MinHeap.Data[p]
    while(parent*2<MinHeap.Size):
        child=parent*2
        if((child!=MinHeap.Size)and(MinHeap.Data[child]>MinHeap.Data[child+1])):
            child+=1
        if(X<=MinHeap.Data[child]):break
        else:MinHeap.Data[parent]=MinHeap.Data[child]
        parent=child

    MinHeap.Data[parent]=X
    return

# def BuildHeap(MinHeap):
#     i=MinHeap.Size//2
#     for index in range(i,0):
#         PercDown(MinHeap,i)

def BuildHeap(List):
    MinHeap=CreatHeap(len(List)+1)
    for i in List:
        Insert(MinHeap,i)
    node=MinHeap.Size//2
    for index in range(node,0):
        PercDown(MinHeap,index)


class HuffmanTree:
    """树类"""
    def __init__(self):
        self.root = HNode()
        self.myQueue=[]


#根据一个列表创建哈夫曼树
def HuffmanCreat(H):
    BuildHeap(H)
    T=HuffmanTree()
    while(len(H)>0):
        leftnode=HNode(DeleteMin(H))
        rightnode=HNode(DeleteMin(H))
        newnode=HNode(leftnode.value+rightnode.value)
        newnode.left=leftnode
        newnode.right=rightnode
        Insert(H,leftnode.value+rightnode.value)

    return T


