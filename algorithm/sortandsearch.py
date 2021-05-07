#-*- coding:utf-8 -*-
#Author:李明洲
#python 3.7.3
#@Time:2020/6/12 16:13

import hashlib
def binarySearch(alist,item):
    if len(alist) == 0:
        return False
    else:
        mid = len(alist)//2
        if alist[mid] == item:
            return True
        else:
            if item < alist[mid]:
                return binarySearch(alist[:mid],item)
            else:
                return binarySearch(alist[mid+1:],item)

# print(binarySearch([1,2,3,4,5],0))

#带有提前退出的改进版本冒泡排序
def bubbleSort(alist):
    exchange = True
    passnum = len(alist)-1
    while passnum>0 and exchange:
        exchange = False
        for i in range(passnum):
            if alist[i] > alist[i+1]:
                exchange = True
                # temp = alist[i]
                # alist[i] = alist[i+1]
                # alist[i+1] = temp
                #python支持直接交换
                alist[i],alist[i+1] = alist[i+1],alist[i]
        passnum = passnum - 1

#选择排序 减少了交换 每次只选择最大的放后面
def selectionSort(alist):

    for fillslot in range(len(alist)-1,0,-1):
        positionofMax = 0
        print(alist)
        #注意 将对比值设为alist[0] 就可以从1开始迭代了
        for location in range(1,fillslot+1):
            if alist[location] > alist[positionofMax]:
                positionofMax = location
        alist[fillslot],alist[positionofMax] = alist[positionofMax],alist[fillslot]

#插入排序 因为移动只有一次赋值 因此比交换性能好
def insertionSort(alist):
    for index in range(1,len(alist)):
        currentvalue = alist[index]
        position = index

        while position > 0 and alist[position-1] > currentvalue:
            alist[position] = alist[position-1]
            position = position - 1
        alist[position] = currentvalue

#希尔排序 子列表插入排序再整个插入排序
#需要多看
def ShallSort(alist):
    sublistcount = len(alist)//2
    while sublistcount >0:
        for startposition in range(sublistcount):
            getInsertSort(alist,startposition,sublistcount)
        print("after increments of size",sublistcount,"the list is",alist)
        sublistcount = sublistcount // 2

def getInsertSort(alist,start,gap):
    for index in range(start+gap,len(alist),gap):
        print(index)
        currentvalue = alist[index]
        position = index

        while position >= gap and alist[position-gap] > currentvalue:
            alist[position] = alist[position-gap]
            position = position - gap
        alist[position] = currentvalue

#归并排序 使用递归的思想
#基础代码

# def MergeSort(alist):
#     if len(alist)>1:
#         mid = len(alist) // 2
#         lefthalf = alist[:mid]
#         righthalf = alist[mid:]
#
#         MergeSort(lefthalf)
#         MergeSort(righthalf)
#
#         i= j= k= 0
#         #双指针拉链式合并排序
#         while i <len(lefthalf) and j < len(righthalf):
#             if lefthalf[i] < righthalf[j]:
#                 alist[k] = lefthalf[i]
#                 i = i+1
#             else:
#                 alist[k] = righthalf[j]
#                 j = j+1
#             k = k+1
#
#         while i < len(lefthalf):
#             alist[k] = lefthalf[i]
#             i = i+1
#             k = k+1
#
#         while j < len(righthalf):
#             alist[k] = righthalf[j]
#             j = j+1
#             k = k+1
#pythonic风格代码
def MergeSort(alist):
    if len(alist) <= 1:
        return alist

    mid = len(alist) // 2
    left = MergeSort(alist[:mid])
    right = MergeSort(alist[mid:])

    merged = []
    while left and right:
        if left[0] <= right[0]:
            merged.append(left.pop(0))
        else:
            merged.append(right.pop(0))
    merged.extend(right if right else left)
    return merged

#快速排序
#核心是中值的选择，不然可能退化为n2级别
#多看
def quickSort(alist):
    quickSortHelper(alist,0,len(alist)-1)

def quickSortHelper(alist,first,last):
    if first < last:
        splitpoint = partition(alist,first,last)
        quickSortHelper(alist,first,splitpoint-1)
        quickSortHelper(alist,splitpoint+1,last)

def partition(alist,first,last):
    pivotvalue = alist[first]
    leftmark = first +1
    rightmark = last
    done = False
    while not done:
        while leftmark <= rightmark and alist[leftmark] <= pivotvalue:
            leftmark = leftmark+1
        while alist[rightmark] >= pivotvalue and rightmark >= leftmark:
            rightmark = rightmark-1
        print(leftmark,rightmark)
        if rightmark < leftmark:
            done = True
        else:
            temp = alist[leftmark]
            alist[leftmark] = alist[rightmark]
            alist[rightmark] = temp


    temp = alist[first]
    alist[first] = alist[rightmark]
    alist[rightmark] = temp

    return rightmark

#堆排序
def heap_sort(nums):
    # 调整堆
    # 迭代写法
    def adjust_heap(nums, startpos, endpos):
        newitem = nums[startpos]
        pos = startpos
        childpos = pos * 2 + 1
        while childpos < endpos:
            rightpos = childpos + 1
            if rightpos < endpos and nums[rightpos] >= nums[childpos]:
                childpos = rightpos
            if newitem < nums[childpos]:
                nums[pos] = nums[childpos]
                pos = childpos
                childpos = pos * 2 + 1
            else:
                break
        nums[pos] = newitem

    # 递归写法
    def adjust_heap(nums, startpos, endpos):
        pos = startpos
        chilidpos = pos * 2 + 1
        if chilidpos < endpos:
            rightpos = chilidpos + 1
            if rightpos < endpos and nums[rightpos] > nums[chilidpos]:
                chilidpos = rightpos
            if nums[chilidpos] > nums[pos]:
                nums[pos], nums[chilidpos] = nums[chilidpos], nums[pos]
                adjust_heap(nums, pos, endpos)

    n = len(nums)
    # 建堆
    for i in reversed(range(n // 2)):
        adjust_heap(nums, i, n)
    # 调整堆
    for i in range(n - 1, -1, -1):
        nums[0], nums[i] = nums[i], nums[0]
        adjust_heap(nums, 0, i)
    return nums

#计数排序
def counting_sort(nums):
    if not nums: return []
    n = len(nums)
    _min = min(nums)
    _max = max(nums)
    tmp_arr = [0] * (_max - _min + 1)
    for num in nums:
        tmp_arr[num - _min] += 1
    j = 0
    for i in range(n):
        while tmp_arr[j] == 0:
            j += 1
        nums[i] = j + _min
        tmp_arr[j] -= 1
    return nums

#桶排序
def bucket_sort(nums, bucketSize):
    if len(nums) < 2:
        return nums
    _min = min(nums)
    _max = max(nums)
    # 需要桶个数
    bucketNum = (_max - _min) // bucketSize + 1
    buckets = [[] for _ in range(bucketNum)]
    for num in nums:
        # 放入相应的桶中
        buckets[(num - _min) // bucketSize].append(num)
    res = []

    for bucket in buckets:
        if not bucket: continue
        if bucketSize == 1:
            res.extend(bucket)
        else:
            # 当都装在一个桶里,说明桶容量大了
            if bucketNum == 1:
                bucketSize -= 1
            res.extend(bucket_sort(bucket, bucketSize))
    return res

#基数排序
def Radix_sort(nums):
    if not nums: return []
    _max = max(nums)
    # 最大位数
    maxDigit = len(str(_max))
    bucketList = [[] for _ in range(10)]
    # 从低位开始排序
    div, mod = 1, 10
    for i in range(maxDigit):
        for num in nums:
            bucketList[num % mod // div].append(num)
        div *= 10
        mod *= 10
        idx = 0
        for j in range(10):
            for item in bucketList[j]:
                nums[idx] = item
                idx += 1
            bucketList[j] = []
    return nums



# alist = [0,1,1,0,2,5]
# heap_sort(alist)
# print(alist)


# md5 = hashlib.md5()
# md5.update("a".encode('utf-8'))
# print(u"digest返回的摘要：%s"% md5.digest())
# print(u"hexdigest返回的摘要：%s"% md5.hexdigest())



class HashTable:
    def __init__(self):
        self.size = 11#素数
        self.slots = [None]*self.size
        self.data = [None]*self.size

    def hashfunction(self,key):
        return key% self.size

    def rehash(self,oldhash):
        return (oldhash+ 1)% self.size

    def put(self,key,data):
        hashvalue = self.hashfunction(key)

        if self.slots[hashvalue] == None:
            self.slots[hashvalue] = key
            self.data[hashvalue] = data
        else:
            if self.slots[hashvalue] == key:
                self.data[hashvalue] = data
            else:
                nextslot = self.rehash(hashvalue)
                while self.slots[nextslot] != None and \
                    self.slots[nextslot] != key:
                    nextslot = self.rehash(nextslot)

                if self.slot[nextslot] == None:
                    self.slot[nextslot] = key
                    self.data[nextslot] = data
                else:
                    self.data[nextslot] = data

    def get(self,key):
        startslot = self.hashfunction(key)

        data = None
        stop = False
        found = False
        position = startslot
        while self.slots[position] !=None and\
            not found and not stop:
            if self.slots[position] == key:
                found = True
                data = self.data[position]
            else:
                position = self.rehash(position)
                if position == startslot:
                    stop = True
        return data

    def _getitem_(self,key):
        return self.get(key)
    def _setitem_(self,key,data):
        self.put(key,data)
        

