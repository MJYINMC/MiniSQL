from Utils.Utils import *
from Global.DataExchange import *
from OtherException.Exception import *
from BufferManager.BufferManager import BufferManager as bf
from RecordManager.RecordManager import RecordManager as rm

import os

debug = False
"""
Catalog Managerr负责管理数据库的所有模式信息
用于从缓冲区获得原始数据并转换为API可以接受的形式
以及提供一些API所需的函数
"""
class CatalogManager:
    file_name = RELATION_META_FILE_NAME
    header = None
    result = None

    @classmethod
    def sync(cls):
        cls._init_catalog()
        cls.result = cls._scan_file()

    @classmethod
    def select(cls, condition = None,  mode = SELECT_TABLLE):
        """
        :param condition Condition[] 可以包含多个比较条件的列表
        :param mode      int 为0代表选择table_name, 为1代表选择attributes
        return table_name[] 或者 attributes[]
        table_name示例:
        [
        [1,"stu",]
        [2,"teacher"]
        ]
        attributes示例:
        [
        [id, int, True, True, 2333],
        [name, char(5), False, False, 0]    
        ]
        """
        if(cls.header == None):
            cls._init_catalog()
        if(cls.result == None):
            cls.result = cls._scan_file()
        
        table_list = cls._filter_data(condition)
        if(mode == SELECT_TABLLE):
            return table_list
        elif(mode == SELECT_ATTR and table_list != []):
            return cls.result[condition.value][1]
        else:
            return []

    @classmethod
    def insert(cls, table_name, attributes, primary_key):
        if(cls.header == None):
            cls._init_catalog()
        page_id = bf.allocate_available_page(cls.file_name)
        valid = bool_to_byte(True)
        table_id = int_to_byte(page_id)
        name = str_to_byte(table_name, 256)
        attr_num = int_to_byte(len(attributes))
        attr_size = int_to_byte(ATTR_SIZE)
        attr_array = []
        attributes_data = b''
        record_length = 4
        for i in attributes:
            attr_info, byte_array, attr_length = cls._attr_to_byte(i, primary_key)
            attributes_data += byte_array
            record_length += attr_length
            attr_array.append(attr_info)
        data = valid + table_id + name + attr_num + attr_size + attributes_data
        page_data = PageData(-1, bytearray(data + (4092 - len(data))*b'\x00'))
        bf.set_page(cls.file_name, page_id, page_data)
        cls.header.data[0:4] = int_to_byte(byte_to_int(cls.header.data[0:4]) + 1)
        cls.result[table_name] = (page_id, attr_array)
        rm.create_table(table_name, bytearray(attr_num + attr_size + attributes_data), record_length)
        
        """
        更新第一个可用页
        由于每一个表格记录需要一页,每次都要进行更新
        """
        flag = False
        for i in range(page_id, cls.header.size + 1):
            page_data = bf.get_pagedata(cls.file_name, i)
            if page_data.first_free_record != -1:
                cls.header.first_free_page = i
                # 找到了可用页
                flag = True
                break
        if flag == False:
            cls.header.first_free_page = 0
        bf.make_file_dirty(cls.file_name)
        return True

    @classmethod
    def delete(cls, table_name):
        if(cls.header == None):
            cls._init_catalog()
        if(cls.result == None):
            cls.result = cls._scan_file()
        page_id =  cls.result[table_name][0]
        if cls.header.first_free_page == 0:
            cls.header.first_free_page = page_id
        elif page_id < cls.header.first_free_page:
            cls.header.first_free_page = page_id
        cls.header.data[0:4] = int_to_byte(byte_to_int(cls.header.data[0:4]) - 1)
        bf.make_file_dirty(cls.file_name)
        bf.mark_record_invalid(cls.file_name, page_id, 0, 3984)
        cls.result.pop(table_name)
        return True

    @classmethod
    def set_index(cls, index_name, index_id, table_name, attr_name):
        if(cls.header == None):
            cls._init_catalog()
        if(cls.result == None):
            cls.result = cls._scan_file()
        page_id = cls.result[table_name][0]
        attr_list = cls.result[table_name][1]
        for i in range(len(attr_list)):
            if attr_list[i][0] == attr_name:
                if attr_list[i][-1] != 0:
                    raise SyntaxError("Another index already exists on %s(%s)"%(table_name, attr_name))
                elif attr_list[i][-2] != True:
                    raise SyntaxError("Index can only build on unique attributes")               
                else:
                    page_data = bf.get_pagedata(cls.file_name, page_id)
                    page_data.data[272+(i+1)*116-4:272+(i+1)*116] = int_to_byte(index_id)
                    bf.make_block_dirty(cls.file_name, page_id)
                    attr_list[i][-1] = index_id
                    break
        return rm.set_index(index_name, index_id, table_name, attr_name)

    @classmethod
    def remove_idx(cls, table_name, attr_name):
        if(cls.header == None):
            cls._init_catalog()
        if(cls.result == None):
            cls.result = cls._scan_file() 
        page_id = cls.result[table_name][0]
        attr_list = cls.result[table_name][1] 
        for i in range(len(attr_list)):
            if attr_list[i][0] == attr_name:
                page_data = bf.get_pagedata(cls.file_name, page_id)
                page_data.data[272+(i+1)*116-4:272+(i+1)*116] = int_to_byte(0)
                bf.make_block_dirty(cls.file_name, page_id)
                attr_list[i][-1] = 0
                break
        return rm.remove_index(table_name, attr_name)
        
    @classmethod
    def _filter_data(cls, condition):
        result = []
        if condition != None:
            for key in cls.result:
                if key == condition.value:
                    return [[cls.result[key][0], key]]
            return []
        if condition == None:
            for key in cls.result:
                result.append([cls.result[key][0], key])
        return result
    
    @classmethod
    def _attr_to_byte(cls, attr, primary_key):
        """
        接受一个属性的信息
        转换成116字节的bytearray
        """
        is_prk = False
        attr_byte = b''
        attr_byte += str_to_byte(attr.name, 96)
        attr_byte += int_to_byte(attr.attr_type)
        attr_byte += int_to_byte(attr.length)
        if attr.name == primary_key:
            attr_byte += bool_to_byte(True)
        else:
            attr_byte += bool_to_byte(False)
        attr_byte += bool_to_byte(attr.unique)
        attr_byte += int_to_byte(attr.index_id)

        if(attr.attr_type == AttrType.int_type.value):
            attr_type = "int"
        elif(attr.attr_type == AttrType.char_type.value):
            attr_type = "char("+str(attr.length)+")"
        elif(attr.attr_type == AttrType.float_type.value):
            attr_type = "float"
        if(attr.name == primary_key):
            is_prk = True
        attr_info = [attr.name, attr_type, is_prk, attr.unique, attr.index_id]
        return attr_info, attr_byte, attr.length

    @classmethod
    def _scan_file(cls, key = None):
        """
        param: key为None全局查找，不为None时则查找到key终止
        返回所有的表名和模式(字典)
        table_name : (table_id, attribues)
        """
        result = {}
        for id in range(1, cls.header.size + 1):
            page_data = bf.get_pagedata(cls.file_name, id)
            if(byte_to_bool(page_data.data[0:4]) == True):
                table_id = byte_to_int(page_data.data[4:8])
                table_name = byte_to_str(page_data.data[8:264])
                if(key != None and key == table_name):
                    return table_id
                attr_num = byte_to_int(page_data.data[264:268])
                attr_size = byte_to_int(page_data.data[268:272])
                attr_list = []
                for j in range(0, attr_num):
                    offset = 272 + j * attr_size
                    attr = page_data.data[offset: offset + attr_size]
                    attr_list.append(cls._scan_attr(attr))
                result[table_name] = (table_id, attr_list)
        if(key == None):
            return result
        else:
            return 0

    @classmethod
    def _scan_attr(cls, record):
        """
        接受116字节的数据，转换为属性列表
        :param: record byte[]
        return 属性列表
        """
        Field = byte_to_str(record[0:96])
        Type = byte_to_int(record[96:100])
        Length = byte_to_int(record[100:104])
        Primary_key = byte_to_bool(record[104:108])
        Unique = byte_to_bool(record[108:112])
        Index_id = byte_to_int(record[112:116])
        if(Type == AttrType.int_type.value):
            Type = "int"
        elif(Type == AttrType.char_type.value):
            Type = "char("+str(Length)+")"
        elif(Type == AttrType.float_type.value):
            Type = "float"
        return [Field, Type, Primary_key, Unique, Index_id]
    
    @classmethod
    def _init_catalog(cls):
        """
        如果未创建磁盘文件，则创建文件并读入内存
        否则 将文件读入缓冲区文件列表中
        """
        if not os.path.isfile(os.path.join(DBFILES_FOLDER, cls.file_name)):
            cls.header = cls._create_header()
            bf.create_file(cls.file_name, cls.header)
        else:
            cls.header = bf.get_file_header(cls.file_name)

    @classmethod
    def _create_header(cls):
        """
        未创建任何表格时，先创建一个空表头
        """
        '''
        table_num = 0
        index_num = 0
        record_length = 3984
        record_capacity = 1
        '''
        data = bytearray(int_to_byte(0) + int_to_byte (0) + int_to_byte(3984) + int_to_byte(1) + b'\x00'*4072)
        '''
        First free page = 0
        Capacity = MAX_CAPACITY
        Size = 0
        '''
        header = PagedFileHeader(0, MAX_CAPACITY, 0, data)
        return header