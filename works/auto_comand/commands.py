import numpy

class Grade_performance():
    def __init__(self,point):
        if(point==1):
            self.comand="成绩优秀，基本能正确审题，分析能力强，具有本学科学习潜力。\n"
        elif(point==2):
            self.comand="本次成绩良好，在基础知识方面还存在一部分可以掌握，一部分掌握不扎实的情况，分析能力有待加强。\n"
        elif(point==3):
            self.comand="本次成绩不太理想，基础知识的记忆和表达上有较多疏漏，需要努力的方向还有很多（词汇，知识点记忆，基本表达，分析题目的方法）\n"
        elif(point==4):
            self.comand="本次成绩较差，基础知识记忆和复述存在很大问题，基本的词汇和知识点无法完整叙述。\n"
        else:
            self.comand=''


class Classroom_performance():
    def __init__(self, point):
        if (point == 1):
            self.comand = "在平时学习中，课堂表现很好，能跟住上课节奏，积极参加课堂活动和发言，学习习惯好，能及时整理笔记，对本学科体现出很高的学习热情。\n"
        elif (point == 2):
            self.comand = "在平时学习中，课堂表现较好，能理解课堂的基本内容，积极参加课堂活动和发言，但笔记记录不积极，需要经常提醒。可能因此产生复习效果较差或者知识记忆效果不佳。\n"
        elif (point == 3):
            self.comand = "在平时学习中，课堂表现尚可，不爱发言，但是能基本跟住上课节奏，理解上课内容，记笔记积极度一般，需要经常提醒。可能因此产生复习效果较差或者知识记忆效果不佳。\n"
        elif (point == 4):
            self.comand = "在平时学习中，注意力集中程度较差，经常需要老师提醒，可能存在由此知识点的理解和记忆不足。\n"
        elif (point == 5):
            self.comand = "平时学习中状态不太稳定，有些时候状态很好，听课效果也佳（但是笔记情况不是特别理想，还需要重视），有些时候上课溜号，或和同学讲话过多，影响自己的学习状态。\n"
        else:
            self.comand=''

class Homework_performance():
    def __init__(self, point):
        if (point == 1):
            self.comand = "作业完成十分认真及时，可以看出很用心去做了，也能根据老师的评语及时订正，学习习惯很好。\n"
        elif (point == 2):
            self.comand = "作业完成较为认真，大部分情况下能正常完成，及时订正。\n"
        elif (point == 3):
            self.comand = "作业完成情况一般，有不交的情形出现，反馈不够及时，需要改进。\n"
        elif(point==4):
            self.comand = "作业完成情况较差，经常不交。\n"
        else:
            self.comand = ''


class improvement():
    def __init__(self, point):
        if (point == 1):
            self.comand = "当前学习状态很好，希望能在下半学期中继续保持，在夯实基础知识的基础上，多思考，钻研答题方法。学有余力的话建议阅读一些自己感兴趣的历史书籍或相关知识。\n"
        elif (point == 2):
            self.comand = "当前学习状态比较好，希望下学期中可以继续努力，有不懂的问题及时问，在扎实知识的同时注重答题方法的练习，进一步端正学习态度，还有很大的学习潜力。尤其应该重视两点：作业的完成质量和反馈，收到批改后要及时思考，改正。上课时生词和新知识的及时记录，以减轻复习压力，更高效地学习。\n"
        elif (point == 3):
            self.comand = "当前学习状态一般，在语言方面尤其需要注意加强，平时注意词汇及时积累，记录和记忆，复习，注意积累常见表达句型（比如一般如何评述，如何论证等），我们平时的练习对学生英语词汇和写作的帮助都很大，希望能进一步端正学习态度，在下半个学期中取得长足进步。\n"
        elif(point==4):
            self.comand="当前面临的学习困难较多，希望能争取克服，在端正学习态度的同时，加强上课注意力和记笔记，这是历史学科本阶段最重要的内容：夯实基础知识。上课时生词和新知识需要及时记录，及时复习。当前如果能在词汇和句子表达上注意积累，练习，相信会有很大进步。\n"
        else:
            self.comand=''

filename='./comands.txt'
pointname='./point.txt'

with open(pointname) as file_object:
    for line in file_object:
        code=int(line)
        grade_point=code//1000
        classroom_point = (code - grade_point * 1000) // 100
        student_Homework_point = int((code % 100) / 10)
        improvement_point = code % 10

        student_grade = Grade_performance(grade_point)
        student_Classroom = Classroom_performance(classroom_point)
        student_Homework = Homework_performance(student_Homework_point)
        student_improvement = improvement(improvement_point)

        comands=student_grade.comand+student_Classroom.comand+student_Homework.comand+student_improvement.comand
        with open(filename, 'a') as file_object:
            file_object.write(comands)
            file_object.write("\n\n")

# prompt = "\ninput the student code,input 0000 to quit:"
# message = ""
# while message != '0000':
#     message = input(prompt)
#     code=int(message)
#
#     grade_point=code//1000
#     classroom_point = (code - grade_point * 1000) // 100
#     student_Homework_point = int((code % 100) / 10)
#     improvement_point = code % 10
#
#     student_grade = Grade_performance(grade_point)
#     student_Classroom = Classroom_performance(classroom_point)
#     student_Homework = Homework_performance(student_Homework_point)
#     student_improvement = improvement(improvement_point)
#
#     comands=student_grade.comand+student_Classroom.comand+student_Homework.comand+student_improvement.comand
#     with open(filename, 'a') as file_object:
#         file_object.write(comands)
#         file_object.write("\n\n")




