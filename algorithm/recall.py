#-*- coding:utf-8 -*-
#Author:李明洲
#python 3.7.3
#@Time:2020/6/12 16:21



# class Solution(object):
#     def __init__(self):
#         self.ans = []
#
#     def Nqueens(self, queenlist, line, queennum):
#         if line == queennum:
#             temp = []
#             for i in range(queennum):
#                 thisline = ''
#                 for j in range(queennum):
#                     if [i, j] in queenlist:
#                         thisline += 'Q'
#                     else:
#                         thisline += '+'
#                 temp.append(thisline)
#             self.ans.append(temp)
#
#         else:
#             for i in range(queennum):
#                 is_in = 0
#                 for queen in queenlist:
#                     if i == queen[1] or abs(line - queen[0]) == abs(i - queen[1]):
#                         is_in = 1
#
#                 if queenlist == [] or is_in == 0:
#                     queenlist.append([line, i])
#                     self.Nqueens(queenlist, line + 1, queennum)
#                     queenlist.pop(-1)
#
#     def solveNQueens(self, n):
#         """
#         :type n: int
#         :rtype: List[List[str]]
#         """
#         queenlist = []
#         line = 0
#         self.Nqueens(queenlist, line, n)
#         return self.ans


# class Solution(object):
#     def change(self,code,i):
#         if code[i] == 0:
#             code[i] = 1
#         else:
#             code[i] =0
#
#     def grayCode(self, n):
#         Gray = [[0]*n]
#         ans = []
#         for i in range(1,pow(2,n)):
#             temp = Gray[-1].copy()
#             for j in range(n-1,-1,-1):
#                 thiscode = temp.copy()
#                 self.change(thiscode,j)
#                 if thiscode not in Gray:
#                     Gray.append(thiscode)
#                     break
#         for code in Gray:
#             num=0
#             for i in range(len(code)-1,-1,-1):
#                 num += code[i]*pow(2,len(code)-i-1)
#             ans.append(num)
#         return Gray


class Solution:
    def permute(self, nums):
        def backtrack(first=0):
            # 所有数都填完了
            if first == n:
                res.append(nums[:])
            for i in range(first, n):
                # 动态维护数组
                nums[first], nums[i] = nums[i], nums[first]
                # 继续递归填下一个数
                backtrack(first + 1)
                # 撤销操作
                nums[first], nums[i] = nums[i], nums[first]

        n = len(nums)
        res = []
        backtrack()
        return res

grid = [[1, 0, 7], [2, 0, 6], [3, 4, 5], [0, 3, 0], [9, 0, 20]]
nums=[1,2,3]
solution = Solution()
print(solution.permute(nums))

