import random
# file = open("record_test.txt", "w")
# file.write("create table stu(id int, name char(50), score float, test int unique, primary key(id));\n")
# for i in range(1, 100000):
#     file.write("insert into stu values(%d, \"test\", 66.66, %d);\n"%(i,i))
# file.close()

file = open("record_test.txt", "w")
file.write("create table stu(id int, name char(50), score float, primary key(id));\n")
l = []
for i in range(1, 10000):
    id = random.randint(0, 999999)
    if id not in l:
        file.write("insert into stu values(%d, \"test\", 66.66);\n"%(id))
    l.append(id)
file.close()

file = open("record_test.txt", "w")
file.write("create table stu(id int, name char(50), score float unique, primary key(id));\n")
l = []
for i in range(1, 10000):
    id = random.randint(0, 999999)
    if id not in l:
        file.write("insert into stu values(%d, \"test\", %f);\n"%(id, i))
    l.append(id)
file.close()
