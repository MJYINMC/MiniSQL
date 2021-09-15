from enum import Enum
import os
from Global.DataExchange import AttrType
from struct import pack, unpack

# 这个文件用于放全局变量、常数、枚举和各种通用的函数

# 这里是一个page的大小
PAGE_SIZE = 4096

# 这里是一个attribute的大小
ATTR_SIZE = 116

# 这里是meta文件的名字
RELATION_META_FILE_NAME = "table_meta.db"

# 这里是DBFiles的目录
DBFILES_FOLDER = "DBFiles"

# 这里是meta文件的目录
TABLE_META_FILE = os.path.join(DBFILES_FOLDER, RELATION_META_FILE_NAME)

# 这里定义单表文件的最大大小，大概为3.2G
MAX_CAPACITY = 800000

# 以下为byte与int,str,float类型的各种转化函数
def str_to_byte(str, length):
    encoded = str.encode('unicode_escape')  
    return encoded + (length - len(encoded)) * b'\x00'

def byte_to_str(byte):
    return byte.decode('unicode_escape').strip(b'\x00'.decode())

def int_to_byte(i):
    if i is None:
        i = 0
    return int(i).to_bytes(length=4, byteorder='big', signed=True)

def byte_to_int(byte):
    i = int().from_bytes(byte, byteorder='big', signed=True)
    return i

def float_to_byte(float):
    return pack('f', float)

def byte_to_float(byte):
    return unpack('f', byte)[0]

def bool_to_byte(bool):
    return int(bool).to_bytes(length=4, byteorder='big', signed=True)

def byte_to_bool(byte):
    i = int().from_bytes(byte, byteorder='big', signed=True)
    return bool(i)

def key_to_byte(key_type, key, length = None):
    """
    根据类型转化方式不同
    :param key_type: 1:int, 2:str, 3:float
    :param key:     需要被转化的值
    :return:        转化之后的byte[]
    """
    if key_type == AttrType.int_type.value:
        return int_to_byte(key)
    elif key_type == AttrType.char_type.value:
        return str_to_byte(key, length)
    elif key_type == AttrType.float_type.value:
        return float_to_byte(key)
    elif key_type == AttrType.bool_type.value:
        return bool_to_byte(key)

def byte_to_key(key_type, byte):
    """
    根据类型不同转化方式不同
    :param key_type: 1:int, 2:str, 3:float, 4:bool
    :param byte:     需要被转化的byte[]
    :return:         被转化之后的值
    """
    if key_type == AttrType.int_type.value:
        return byte_to_int(byte)
    elif key_type == AttrType.char_type.value:
        return byte_to_str(byte)
    elif key_type == AttrType.float_type.value:
        return byte_to_float(byte)
    elif key_type == AttrType.bool_type.value:
        return byte_to_bool(byte)