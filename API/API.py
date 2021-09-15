from math import tan
from os import error
from select import select
import struct
from Utils.Utils import bool_to_byte, float_to_byte, int_to_byte, key_to_byte, str_to_byte
import re
from OtherException.Exception import *
from Global.DataExchange import *
from CatalogManager.CatalogManager import CatalogManager as cm
from BufferManager.BufferManager import BufferManager as bf
from RecordManager.RecordManager import RecordManager as rm
from IndexManager.IndexManager import IndexManager as im
import struct

debug = False
def api_create_table(table_name, attributes, primary_key):
    """
    :param table_name: string，存放表的名字
    :param attributes: Attribute[] 存放各个属性的各项信息
    :param primary_key: string，存放主键名字
    :return: 依据各个manager的返回值
    """
    # 调用catalog manager 检查是否有同名表存在
    condition = Condition("table_name", "=", table_name)
    result = cm.select(condition, SELECT_TABLLE)
    if result == []: # 如果没有同名表存在
        index_id = im.get_new_index_id()
        index_name = "auto_" + table_name
        for i in attributes:
            if i.name == primary_key:
                primary_attr = i
                i.index_id = index_id
                break
        im.create_index(index_name, index_id, table_name, primary_attr.name, primary_attr.attr_type, primary_attr.length )
        cm.insert(table_name, attributes, primary_key)
        return 1
    else:
        raise SyntaxError("Table '%s' already exists" % (table_name))

def api_drop_table(table_name):
    condition = Condition("table_name", "=", table_name)
    result = cm.select(condition, SELECT_TABLLE)
    if result == []: # 不存在该表
        raise SyntaxError("Table '%s' doesn't exists" % (table_name))
    else:
        return cm.delete(table_name)

def api_desc(table_name):
    """
    :param table_name:  string 表的名字
    return 表的属性信息
    """
    condition = Condition("table_name", "=", table_name)
    result = cm.select(condition, SELECT_ATTR)
    if result == []: # 不存在该表
        raise SyntaxError("Table '%s' doesn't exists" % (table_name))
    else:
        # table_info和result均为列表嵌套，使用 + 运算符进行连接
        table_info = ["Field", "Type", "Primary_key", "Unique", "Index_id"]
        return ([table_info] + result, table_info)

def api_show_table():
    # Condition 为None时代表选择所有
    result = cm.select(None, SELECT_TABLLE)
    table_info = [["Table_id", "Table_name"]]
    return table_info + result

def api_show_index():
    return True

def api_insert(table_name, values):

    condition = Condition("table_name", "=", table_name)
    result = cm.select(condition, SELECT_ATTR)
    if debug:
        print(result)
    if result == []: # 不存在该表
        raise SyntaxError("Table '%s' doesn't exists" % (table_name))
    else:
        byte_values = []
        # 检查各项输入的属性是否正确
        if(len(result) != len(values)):
            raise SyntaxError("Table '%s' expects %d attributes, but %d are given" % (table_name, len(result), len(values)))
        # 本条记录有效, valid为True
        byte_values = bytearray(bool_to_byte(True)) 
        for i in range(len(result)):
            try:
                if result[i][1] == 'int':
                    byte_data = int_to_byte(int(values[i]))
                elif result[i][1] == 'float':
                    byte_data = float_to_byte(float(values[i]))
                else:
                    str_match = re.match(r'^\"(.+)\"$', values[i], re.S)
                    if str_match == None:
                        str_match = re.match(r'^\'(.+)\'$', values[i], re.S)
                    if str_match == None:
                        raise SyntaxError("Data truncated for column '%s'\nUse 'desc %s' for help" % (result[i][0], table_name))
                    str = str_match.group(1)
                    match = re.match(r'^char\s*\(\s*([+,-]*[0-9]+)\s*\)$', result[i][1], re.S)
                    max_len = int(match.group(1))
                    if debug:
                        print(str)
                    byte_data = str_to_byte(str, max_len)
                    if len(byte_data) > max_len:
                        raise SyntaxError("Input string exceeds max length '%d' restriction" % (max_len))
                byte_values += byte_data
            except (TypeError, ValueError, struct.error):
                raise SyntaxError("Data truncated for column '%s'\nUse 'desc %s' for help" % (result[i][0], table_name))
    rm.insert(table_name, byte_values)
    return 1

def api_select(table_name, attribute, select_condition):
    condition = Condition("table_name", "=", table_name)
    result = cm.select(condition, SELECT_TABLLE)
    if result == []: # 不存在该表
        raise SyntaxError("Table '%s' doesn't exists" % (table_name))
    else:
        return rm.select(table_name, attribute, select_condition)

def api_delete(table_name, delete_condition):
    condition = Condition("table_name", "=", table_name)
    result = cm.select(condition, SELECT_TABLLE)
    if result == []: # 不存在该表
        raise SyntaxError("Table '%s' doesn't exists" % (table_name))
    else:
        return rm.delete(table_name, delete_condition)

def api_create_index(index_name, table_name, attr_name):
    condition = Condition("table_name", "=", table_name)
    result = cm.select(condition, SELECT_ATTR)
    if result == []:
        raise SyntaxError("Table '%s' doesn't exists" % (table_name))
    else:
        if im.get_info_by_index_name(index_name) != (None, None):
            raise SyntaxError("Index '%s' already exists" % (index_name))
        index_id = im.get_new_index_id()
        return cm.set_index(index_name, index_id, table_name, attr_name)

def api_drop_index(index_name):
    table_name, attr_name = im.get_info_by_index_name(index_name)
    if table_name == None:
        raise SyntaxError("Index '%s' doesn't exist" % (index_name))
    else:
        im.remove_index(index_name)
        return cm.remove_idx(table_name, attr_name)

def api_commit():
    bf.flush()
    cm.sync()
    im.sync()
    return True

def api_roll_back():
    bf.roll_back()
    cm.sync()
    im.sync()
    return True