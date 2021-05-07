#-*- coding:utf-8 -*-
#Author:李明洲
#@Time:2020/6/8 9:56

#栈的实现 LIFO
#比如网页的“后退”按钮
class Stack:
    def __init__(self):
        self.items=[]

    def isEmpty(self):
        return self.items==[]

    def push(self,item):
        self.items.append(item)

    def pop(self):
        return self.items.pop()

    def peek(self):
        return self.items[-1]

    def size(self):
        return len(self.items)

if __name__ == '__main__':
    #栈的应用一 简单括号匹配
    # def parCheker(symbolstring):
    #     s=Stack()
    #     blananced=True
    #     index=0
    #     while index<len(symbolstring) and blananced:
    #         symbol=symbolstring[index]
    #         if symbol == '(':
    #             s.push(symbol)
    #         else:
    #             if s.isEmpty():
    #                 blananced=False
    #             else:
    #                 s.pop()
    #
    #         index = index+1
    #
    #     if blananced and s.isEmpty():
    #         return True
    #     else:
    #         return False
    # print(parCheker('((()))'))
    # print(parCheker('(()'))

    #修改为多符号匹配
    #层次结构化文档的校验，操作
    #HTML等
    def matches(open,close):
        opens="([{"
        closers=")]}"
        #index()检测字符串中是否包含子字符串 str ，
        # 如果指定 beg，end范围，则检查是否包含在指定范围内。
        #如果包含子字符串返回开始的索引值，否则抛出异常
        return opens.index(open)==closers.index(close)

    def parCheker(symbolstring):
        s=Stack()
        blananced=True
        index=0
        while index<len(symbolstring) and blananced:
            symbol=symbolstring[index]
            if symbol in  '([{':
                s.push(symbol)
            else:
                if s.isEmpty():
                    blananced=False
                else:
                    top=s.pop()
                    if not matches(top,symbol):
                        blananced=False

            index = index+1

        if blananced and s.isEmpty():
            return True
        else:
            return False

    # print(parCheker('{{([][])}()}'))
    # print(parCheker('[{()'))

    #栈的应用二：进制转换
    def divideByN(deNumber,base):
        #为高进制提供形象
        digits="0123456789ABCDEF"
        remask=Stack()

        while deNumber >0:
            rem=deNumber %base
            remask.push(rem)
            deNumber=deNumber//base

        binString=""
        while not remask.isEmpty():
            binString=binString+digits[remask.pop()]

        return binString

    # print(divideByN(42,8))
    # print(divideByN(42,16))

    #应用三：表达式转换
    #中缀表达式转换为前缀/后缀表达式
    def infixToPostfix(infixexpr):
        prec={}
        prec["*"] = 3
        prec["/"] = 3
        prec["+"] = 2
        prec["-"] = 2
        prec["("] = 1
        opStack = Stack()
        postfixlist = []
        tokenList = infixexpr.split()

        for token in tokenList:
            if token in "ABCDEFGHIGKLMNOPQRSTUVWXYZ" or token in "0123456789" or token in "abcdefghigklmnopqrstuvwxyz":
                postfixlist.append(token)
            elif token =='(':
                opStack.push(token)
            elif token == ')':
                topToken = opStack.pop()
                while topToken != '(':
                    postfixlist.append(topToken)
                    topToken=opStack.pop()
            else:
                while(not opStack.isEmpty()) and (prec[opStack.peek()]>=prec[token]):
                    postfixlist.append(opStack.pop())
                opStack.push(token)

        while not opStack.isEmpty():
            postfixlist.append(opStack.pop())
        return " ".join(postfixlist)

    infix="4 * 6 + 2 * 3 "
    postfix=infixToPostfix(infix)

    #后缀表达式求职
    #利用栈的特性不断求值
    def doMath(op,op1,op2):
        if op == "*":
            return op1 * op2
        elif op == "/":
            return op1 / op2
        elif op =="+":
            return op1 + op2
        else:
            return op1 - op2

    def postfixEval(postfixExpr):
        operandStack = Stack()
        tokenList = postfixExpr.split()
        print(tokenList)
        for token in tokenList:
            if token in "0123456789":
                operandStack.push(int(token))
            else:
                operand2 = operandStack.pop()
                operand1 = operandStack.pop()
                result = doMath(token,operand1,operand2)
                operandStack.push(result)
        return operandStack.pop()

    print(postfix)
    print(postfixEval(postfix))


    #???????
    #单调栈的操作

    class SNode:
        def __init__(self):
            self.Stack=[]

        def Push(self,elem):
            self.Stack.append(elem)
        def isEmpty(self):
            return len(self.Stack)==0
        def Pop(self):
            return self.Stack.pop()

        def Top(self):
            return self.Stack[-1]

    def trap(height):
        res = 0
        S = SNode()
        for i in range(0,len(height)):
            while(not S.isEmpty())and (height[S.Top()]<height[i]):
                bottomindex=S.Pop()
                while((not S.isEmpty())and (height[bottomindex]==height[S.Top()])):
                    S.Pop()
                if not S.isEmpty():
                    res += (min(height[S.Top()], height[i]) - height[bottomindex]) * (i - S.Top() - 1)

            S.Push(i)
        return res



    height=[4,3,1,0,1,2,4]
    print(trap(height))