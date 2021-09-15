file = open("catalog_test.txt", "w")
for i in range(1, 10000):
    file.write("create table stu" + str(i) + "(id int, name char(50), socre float, primary key(id));\n")
file.close()

file = open("catalog_test.txt", "w")
for i in range(1, 10005):
    file.write("desc stu" + str(i) +";\n")
file.close()

file = open("catalog_test.txt", "w")
for i in range(5000, 13005):
    file.write("drop table stu" + str(i) +";\n")
file.close()