import re
from enum import Enum
from OtherException.Exception import *

# 本文件定义数据各模块对外数据交换所用的数据格式

# 缓冲区数据相关
class PageData:
    def __init__(self, first_free_record, data):
        """
        初始化函数
        :param status:   int, 
        :param data:     byte[]，4位以后，数据区，根据类型不同数据不同
        """
        self.first_free_record = first_free_record
        self.data = data

class PagedFileHeader:
    def __init__(self, first_free_page, capacity, size, data):
        """
        初始化函数
        :param first_free_page: int，0-3位，此文件中的第一个可用页，初始值设为0
        :param capacity:        int，4-7位，此文件的最大page数
        :param size:            int，8-11位，此文件中已经开辟的页数
        :param data:            byte[]，12位以后，数据区
        成员变量：
        """
        self.first_free_page = first_free_page
        self.capacity = capacity
        self.size = size
        self.data = data

    def set_first_free_page(self, page_id):
        """
        指定文件中的第一个可用页位置
        :param page_id: int，文件中的第一个可用页位置
        :return:
        """
        self.first_free_page = page_id

def condition_parser(str):
    """
    将用户输入的条件转换为Condition对象
    :param str   string
    例如:
    ' student_name = "哈哈哈" and student_id != 319'
    return
    Condition 
    ["student_name", "=" , "哈哈哈"] 
    ["student_name", "==" , "哈哈哈"] 
    """
    begin = 0
    end = 0
    conditions = []
    while end != -1:
        end = str.find(' and ', begin)
        if end == -1:
            conditions.append(str_to_condition(str[begin: len(str)].strip()))
        else:
            conditions.append(str_to_condition(str[begin: end].strip()))
        begin = end + len(' and ')
    return conditions
    
def str_to_condition(str):
    match = re.match(r'^(\w+)\s*([=<>!]{1,2})\s*(\S+)$', str, re.S)
    if match:
        operator = match.group(2)
        if operator in ["=", "<",">","<=",">=","<>","!="]: 
            return Condition(match.group(1), match.group(2), match.group(3))
        else:
            raise SyntaxError("Unknown select condition")
    else:
        raise SyntaxError("Unknown select condition")

# CatalogManager相关
SELECT_TABLLE   = 0
SELECT_ATTR     = 1
SELECT_INDEX    = 2

# 表格相关
class AttrType(Enum):
    int_type = 1
    char_type = 2
    float_type = 3
    bool_type = 4

class Record:
    def __init__(self, attribute_num, attributes):
        """
        初始化函数
        :param attribute_num:   int，属性的个数
        :param attributes:      Attribute[size]，属性
        next_free_record:       int，下一空白记录的位置
        """
        self.attributes = attributes
        self.attribute_num = attribute_num

class Attribute:
    def __init__(self, name, attr_type, length, unique, index_id = 0):
        """
        :param attribute_id: int
        :param table_id:     int
        :param name:         string
        :param attr_type:    int
        :param length:       int
        :param index_id:     int，0则无
        :param unique:       bool
        """

        self.name = name
        self.attr_type = attr_type
        self.length = length
        self.unique = unique
        self.index_id = index_id



class Condition:
    def __init__(self, attr_name, compare, value):
        """
        初始化函数
        :param attr_name:   string，属性名
        :param compare  :   string，比较的符号，有7种：= \ < \ > \ <= \ >= \ <> \ != 
        :param value    :   string，比较的值，后续转为对应类型   
        举例：condition.string = 'id'
             condition.compare = '=='
             condition.key = '23333'
        """
        self.attr_name = attr_name
        self.compare = compare
        self.value = value