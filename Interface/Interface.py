from typing import Match
from Global.DataExchange import AttrType, Attribute, condition_parser, str_to_condition
import re
import time
import os
from API import API
from OtherException.Exception import SyntaxError, OutOfRange
import prettytable as pt

debug = False

def syntax_analysis_exc(query):
    """
    供main()函数调用
    判断语句类型
    将每一句分配给不同类型的语法分析函数
    :param query: string，原始的语句    
    :return: select操作返回为选择的结果列表，列表的第一个元素为属性列表，
              其他操作成功返回true，失败返回false
    """
    str = query.lower().strip(';\n ')
    query = str.split()
    try:
        if str == '':
            raise SyntaxError("Empty query")
        if query[0] == 'insert':
            return insert_check(str)
        elif query[0] == 'select':
            return select_check(str)
        elif query[0] == 'delete':
            return delete_check(str)
        elif query[0] == 'execfile':
            return execfile_check(str)
        elif query[0] == 'create':
            if query[1] == 'table':
                return create_table_check(str)
            elif query[1] == 'index':
                return create_index_check(str)
        elif query[0] == 'drop':
            if query[1] == 'table':
                return drop_table_check(str)
            elif query[1] == 'index':
                return drop_index_check(str)
        elif query[0] == 'show':
            return show_check(str)
        elif query[0] == 'desc':
            return desc_check(str);
        elif query[0] == 'cls' or query[0] == 'clear':
            return clear_screen()
        elif query[0] == 'commit':
            return commit()
        elif query[0] == 'roll' and query[1] == 'back' or query[0] == 'rollback':
            return roll_back()
        elif query[0] == 'quit' or query[0] == 'exit' or query[0] == 'exit()' :
            return None
    except (SyntaxError, OutOfRange) as e:
        print("Error:", format(e))
        return False
    raise SyntaxError('Error Query Type')

def clear_screen():
    if os.name == 'posix':
        os.system('clear')
    else:
        os.system('cls')
    return True

def commit():
    return API.api_commit()

def roll_back():
    return API.api_roll_back()
    
def desc_check(query):
    """
    desc语句的语法检查，后调用API中的api_desc进行后续操作
    :param query:   string，原始语句
    """
    match =  re.match(r'^desc\s+(\S+)\s*$', query, re.S)
    if match:
        table_name = match.group(1)
        return API.api_desc(table_name)
        if(debug):
            print("Table: %s"%(table_name))
        return True
    else:
        raise SyntaxError('Error Query')

def insert_check(query):
    """
    insert语句的语法检查，后调用API中的api_insert进行后续操作
    :param query: string，原始语句
    :return: 操作成功返回true，失败返回false
    """
    match = re.match(r'^insert\s+into\s+(\S+)\s+values\s*\((.+)\)$', query, re.S)
    if match:
        table_name = match.group(1).strip()
        temp_value = match.group(2).split(",")
        values = []
        # values中的元素类型都为字符串
        for v in temp_value:
            v = v.strip()
            if v == '':
                raise SyntaxError('Error Query')
            values.append(v)
        if(debug):
            print("name:", table_name)
            print("values", values)
        return API.api_insert(table_name, values)
    else:
        raise SyntaxError('Error Query')

def select_check(query):
    table_name = None
    condition = None
    attribute = []
    match = re.match(r'^select\s+(.+)\s+from\s+(\S+)\s+where\s+(.+)$', query, re.S)
    if match:
        attr = match.group(1).strip()
        table_name = match.group(2).strip()
        condition = condition_parser(match.group(3).strip())
    else:
        match = re.match(r'^select\s+(.+)\s+from\s+(\S+)$', query, re.S)
        if match:
            attr = match.group(1).strip()
            table_name = match.group(2).strip()
        else:
            raise SyntaxError('Error Query')

    if attr == '*':
        attribute  = None
    else:
        attr = attr.split(',')
        for a in attr:
            a = a.strip()
            if (a == ''):
                raise SyntaxError("Error Query")
            attribute.append(a)
    return API.api_select(table_name, attribute, condition)

def delete_check(query):
    match = re.match(r'^delete\s+from\s+(\S+)\s+where\s+(.+)$', query, re.S)
    if match:
        table = match.group(1).strip()
        condition = condition_parser(match.group(2).strip())
        return API.api_delete(table, condition)
    else:
        match =  re.match(r'^delete\s+from\s+(.+)$', query, re.S)
        if match:
            table = match.group(1).strip()
            condition = None
            return API.api_delete(table, condition)
        else:
            raise SyntaxError('Error Query')

def execfile_check(query):
    match = re.match(r'^execfile\s+(\S+)$', query, re.S);
    if match:
        file = match.group(1)
    else:
        raise SyntaxError("Error Query")
    fopen = open(file, mode='r',encoding='UTF-8')
    query = ''
    count = 1
    while True:
        line = fopen.readline()
        if not line :
            return True
        else:
            query = query + line
            line = line.strip("\n")
            if len(line) > 0 and line[-1] == ";":
                print('Seq %d:' % (count))
                count += 1
                try:
                    return_num = syntax_analysis_exc(query)
                    if type(return_num) is list:
                        print_table(return_num)
                    elif type(return_num) is int:
                        print("SQL execution success!")
                        print("%d row(s) affected"%(return_num))
                    elif return_num == True:
                        print("Success!")
                    elif return_num == False:
                        print ("Failed!")
                    elif return_num == None:
                        return None
                except SyntaxError as e:
                    print("Error:",format(e))
                    print("Failed!")
                    break
                query = ''
    return True

def create_table_check(query):
    if(debug):
        print("Query:", query)
    match = re.match(r'^create\s+table\s+(\w+)\s*\((.+)\)$', query, re.S)
    attributes = []
    attr_namelist = []
    primary_key = None
    if match:
        table_name = match.group(1)
        if(debug):
            print("Table:", table_name)
        info = match.group(2).split(",")
        for i in info:
            i = i.strip()
            if(debug):
                print("String:", i)
            
            # 对付连续两个,,split出的空字符串
            if(i == ''):
                raise SyntaxError('Syntax Error')
            # 检查是否为主键声明
            match_primary_key = re.match(r'primary\s+key\s*\(\s*(\w+)\s*\)', i, re.S)
            # 检查是否重复声明了主键
            if match_primary_key:
                if(primary_key != None):
                    raise SyntaxError("Multiple primary key defined")
                primary_key = match_primary_key.group(1)
                continue

            unique = False
            # 检查是否有unique属性
            if(i.split()[-1] == "unique"):
                # 检查是否为属性声明
                unique = True
                match_attr =  re.match(r'^(\w+)\s+(.+)\s+unique$', i, re.S)
            else:
                # 检查是否为属性声明
                match_attr =  re.match(r'^(\w+)\s+(.+)$', i, re.S)
            
            if(match_attr):
                attr_name = match_attr.group(1)
                data_type = match_attr.group(2)
                if(debug):
                    print("name:", attr_name)
                    print("type:", data_type)
                # 检查是否声明了同名属性
                if(attr_name in attr_namelist):
                    raise SyntaxError("Duplicate column name '%s'" % (attr_name))
                # 检查类型是否正确
                match_char = re.match(r'^char\s*\(\s*([+-]*[0-9]+)\s*\)$', data_type, re.S)
                if(data_type == "int"):
                    attributes.append(Attribute(attr_name, AttrType.int_type.value, 4, unique))
                elif(data_type == "float"):
                    attributes.append(Attribute(attr_name, AttrType.float_type.value, 4, unique))
                elif(match_char != None):
                    size = int(match_char.group(1))
                    if(size >= 1 and size <= 255):
                        attributes.append(Attribute(attr_name, AttrType.char_type.value, size, unique))
                    else:
                        raise SyntaxError("'char[%d]' Size Out of Range" % (size))
                else:
                    raise SyntaxError("Unknown Value Type") 
                attr_namelist.append(attr_name)
                continue
            raise SyntaxError('Syntax Error')
    else:
        raise SyntaxError('Syntax Error')
    
    # 检查是否有主键
    if(primary_key == None):
        raise SyntaxError("No Primary Key Declared")
    elif(primary_key not in attr_namelist):
        raise SyntaxError("Key column '%s' doesn't exist in table" %(primary_key))
    
    for i in attributes:
        if i.name == primary_key:
            # 主键必须为Unique
            i.unique = True
    return API.api_create_table(table_name, attributes, primary_key)

def create_index_check(query):
    match = re.match(r'^create\s+index\s+(\w+)\s+on\s+(\w+)\s*\((\s*\w+\s*)\)$', query, re.S)
    if match:
        index_name = match.group(1).strip()
        table_name = match.group(2).strip()
        attr_name = match.group(3).strip()
        if(debug):
            print("index:%s\ntable:%s\nattr:%s" % (index_name, table_name, attr_name))
        return API.api_create_index(index_name, table_name, attr_name)
    else:
        raise SyntaxError('Error Query')

def drop_table_check(query):
    match = re.match(r'^drop\s+table\s+(\S+)$', query, re.S)
    if match:
        table = match.group(1).strip()
        return API.api_drop_table(table)
    else:
        raise SyntaxError('Error Query')

def drop_index_check(query):
    query = query.lower().strip(';\n ')
    match = re.match(r'^drop\s+index\s+(\w+)$', query, re.S)

    if match:
        index_name = match.group(1)
        return API.api_drop_index(index_name)
    else:
        raise SyntaxError('Error Query')

def show_check(query):
    """
    检查输入是否符合show index或者show table的形式
    """
    if re.match(r'^show\s+tables\s*$', query, re.S):
        return API.api_show_table()
    else:
        raise SyntaxError('Error Query')

def print_table(table_info):
    """
    仿照MySQL的格式打印结果
    """
    if type(table_info) is list:
        tb = pt.PrettyTable()
        tb.field_names = table_info[0]
        for r in table_info[1:]:
            tb.add_row(r)
        print(tb)
        print("%d row(s) in set" % (len(table_info)-1))
    else:
        table_info, attrs = table_info
        tb = pt.PrettyTable()
        tb.field_names = table_info[0]
        for r in table_info[1:]:
            tb.add_row(r)
        print(tb.get_string(fields = attrs))
        print("%d row(s) in set" % (len(table_info)-1))

def start():
    print('Welcome to MiniSQL database CLI!')
    flag = True
    while flag == True:
        print("MiniSQL",end = '')
        query = ''
        while True:
            line = input("\t> ")
            query = query + line
            # 每次的查询结尾必须有需要有;
            if len(line) > 0 and line[-1] == ';':
                try:
                    start = time.time()
                    return_num = syntax_analysis_exc(query)
                    end = time.time()
                    if type(return_num) is list or type(return_num) is tuple:
                        print_table(return_num)
                        print("Execution time: %.2lf s" % (end - start))
                    elif type(return_num) is int:
                        print("SQL execution success!")
                        print("%d row(s) affected"%(return_num))
                        print("Execution time: %.2lf s" % (end - start))
                    elif return_num == True:
                        print ("SQL execution success!")
                        print("Execution time: %.2lf s" % (end - start))
                    elif return_num == False:
                        print ("SQL execution failed!")
                    elif return_num == None:
                        print ("Bye")
                        flag = False
                    break
                except SyntaxError as e:
                    print("SQL execution failed!")
                    print("Error:", format(e))
                    break