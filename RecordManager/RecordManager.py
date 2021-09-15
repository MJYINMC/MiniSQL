from enum import Flag, unique
from os import remove
import re
import copy
from Utils.Utils import *
from Global.DataExchange import *
from OtherException.Exception import *
from BufferManager.BufferManager import BufferManager as bf
from IndexManager.IndexManager import IndexManager as im
import math

debug = False

class TableManager:
    def __init__(self, table_name):
        """
        基本把头文件里面的所有信息读取过来
        接着把表读一遍
        """
        self.table_name = table_name
        self.file_name = self.table_name + "_record.db"
        self.header = bf.get_file_header(self.file_name)
        self.record_length = byte_to_int(self.header.data[256:260])
        self.record_capacity = byte_to_int(self.header.data[260:264])
        self.attribute_num = byte_to_int(self.header.data[264:268])
        self.attribut_size = byte_to_int(self.header.data[268:272])
        if debug:
            print("Record_length:", self.record_length)
            print("Record_capacity:", self.record_capacity)
            print("Attr_num:", self.attribute_num)
            print("Attr_size:", self.attribut_size)
        self.attr_dict = {}
        self.name_to_idx = {}
        begin = 4
        self.title = []
        for i in range(self.attribute_num):
            offset = 272 + i * self.attribut_size
            attr = self.header.data[offset: offset + self.attribut_size]
            Field, Type, Length, Prikey, Unique, Index_id = self._scan_attr(attr)
            if Prikey:
                self.primary_key = Field
                self.primary_index = Index_id
            self.attr_dict[i] = [Field, Type, begin, begin + Length, Prikey, Unique, Index_id]
            self.name_to_idx[Field] = i
            begin += Length
            self.title.append(Field)
        self.title += ["page_id", "record_id"]
        if debug:
            print(self.attr_dict)

    def insert(self, raw_data):
        """
        插入一项记录, 二进制字节流已经由API整理好
        :param raw_data bytearray[]
        """
        if debug:
            print("Input byte length:",len(raw_data))
        
        # 类型检查正确，接下来检查是否存在重复以及调整索引
        for i in range(self.attribute_num):
            #  [Field, Type, begin, begin + Length, Prikey, Unique, Index_id]
            Field, Type, begin, end, Prikey, Unique, Index_id = self.attr_dict[i]
            key = byte_to_key(Type, raw_data[begin:end])
            if Index_id != 0:
                # 存在索引，那么按照索引文件查找属性是否存在即可
                loc = im.search(Index_id, key)
                if loc != None and loc != (0, 0):
                    raise SyntaxError("Column "+Field+" '" + str(key)+"' already exists")
            if Index_id == 0 and Unique == True:
                # 不存在索引，但是属性为unique，需要扫描全表
                print("Build an index on %s(%s) is recommended"%(self.table_name, Field))
                records = []
                for i in range(1, self.header.size + 1):
                    records += self._scan_page(i)
                condition =  Condition(Field, "=", key)
                if self._filter_data(records, [condition]) != []:
                    raise SyntaxError("Column "+Field+" '" + str(key)+"' already exists")

        # 无重复,接下来进行插入操作
        page_id = bf.allocate_available_page(self.file_name)
        page_data = bf.get_pagedata(self.file_name, page_id)
        first_free_record = page_data.first_free_record
        for i in range(self.attribute_num):
            Field, Type, begin, end, Prikey, Unique, Index_id = self.attr_dict[i]
            key = byte_to_key(Type, raw_data[begin:end])
            if Index_id != 0:
                im.insert(Index_id, key, page_id, first_free_record)

        offset = first_free_record * self.record_length
        page_data.data[offset: offset + self.record_length] = raw_data

        bf.make_block_dirty(self.file_name, page_id)
        flag = False
        for i in range(first_free_record + 1, self.record_capacity):
            offset = i * self.record_length
            if byte_to_bool(page_data.data[offset:offset+4]) == False:
                page_data.first_free_record = i
                flag = True
                break
        
        if flag == False:
            # 若在本页没有找到任何空位置, 则本页已经插满
            page_data.first_free_record = -1
            # 在这种情况下，需要对第一个可用页进行更新
            flag = False
            for i in range(page_id, self.header.size + 1):
                page_data = bf.get_pagedata(self.file_name, i)
                if page_data.first_free_record != -1:
                    self.header.first_free_page = i
                    # 找到了可用页
                    flag = True
                    break
            if flag == False:
                self.header.first_free_page = 0
            bf.make_file_dirty(self.file_name)
        if debug:
            print("First free page:", self.header.first_free_page)
            print("First free record:", page_data.first_free_record)

    def select(self, attribute, condition):
        """
        根据attribute和condition选择数据
        :param attribute    string []
        :param condition    Condition []
        """
        # 检查是否查询了不存在的属性
        if attribute != None:
            for i in attribute:
                if i not in self.title:
                    raise SyntaxError("Column '%s' doesn't exist" % (i))
        
        # 检查是否使用了无用的筛选条件
        if condition != None:
            for i in condition:
                if i.attr_name not in self.title:
                    raise SyntaxError("Column '%s' doesn't exist" % (i.attr_name))
        
        # 检查输入查询条件的值是否类型有误
        if condition != None:
            for i in condition:
                idx = self.name_to_idx[i.attr_name]
                attr_type = self.attr_dict[idx][1]
                if attr_type == AttrType.int_type.value:
                    try:
                        i.value = int(i.value)
                    except:
                        raise SyntaxError("Condition does't match with attribute type")
                elif attr_type == AttrType.char_type.value:
                        match = re.match(r'"([^\"]*)\"', i.value, re.S)
                        if match:
                            i.value = match.group(1)
                            continue
                        match = re.match(r"^'(.*)'$", i.value, re.S)
                        if match:
                            i.value = match.group(1)
                        if match == None:
                            raise SyntaxError("Condition does't match with attribute type")
                elif attr_type == AttrType.float_type.value:
                    try:
                        i.value = float(i.value)
                    except:
                        raise SyntaxError("Condition does't match with attribute type")
        
        loc_list = [] # (pid, rid)的数组

        # 存在查询条件, 我们首先进行查询优化,若使用and连接的查询条件中存在建了索引的,则使用索引先筛选一次,再判断其他条件
        flag = False
        # flag 表示是否通过了索引进行筛选
        if condition != None:
            for i in condition:
                attr_name = i.attr_name
                idx = self.name_to_idx[attr_name]
                index_id = self.attr_dict[idx][-1]
                if index_id != 0:
                    flag = True
                    if i.compare == "=":
                        loc = im.search(index_id, i.value)
                        if loc == None:
                            loc_list = []
                            break
                        else:
                            if loc_list == []:
                                loc_list.append(loc)
                            else:
                                if loc in loc_list:
                                    loc_list = [loc]
                                else:
                                    loc_list = []
                                    break
                    
                    elif i.compare == ">":
                        tmp_list = []
                        keys, pointers = im.get_min_info(index_id)
                        # 针对这是一张空表的情况
                        if keys == []:
                            if attribute == None:
                                return [self.title]
                            else:
                                return ([self.title], attribute)
                        for j in range(len(keys)):
                            if keys[j] > i.value:
                                tmp_list.append(pointers[j])
                        if pointers[-1] == None:
                            page_id = 0
                        elif type(pointers[-1]) is not int:
                            page_id = pointers[-1].page_id
                        else:
                            page_id = pointers[-1]
                        keys, pointers = im.get_leaf_info(index_id, page_id)
                        while keys != []:
                            for j in range(len(keys)):
                                if keys[j] > i.value:
                                    tmp_list.append(pointers[j])
                            page_id = pointers[-1]
                            keys, pointers = im.get_leaf_info(index_id, page_id)                        
                        if loc_list == []:
                            loc_list = tmp_list
                        else:
                            loc_list = list(set(loc_list).intersection(tmp_list))
                            if loc_list == []:
                                break

                    elif i.compare == ">=":
                        tmp_list = []
                        keys, pointers = im.get_min_info(index_id)
                        # 针对这是一张空表的情况
                        if keys == []:
                            if attribute == None:
                                return [self.title]
                            else:
                                return ([self.title], attribute)
                        for j in range(len(keys)):
                            if keys[j] >= i.value:
                                tmp_list.append(pointers[j])
                        if pointers[-1] == None:
                            page_id = 0
                        elif type(pointers[-1]) is not int:
                            page_id = pointers[-1].page_id
                        else:
                            page_id = pointers[-1]
                        keys, pointers = im.get_leaf_info(index_id, page_id)
                        while keys != []:
                            for j in range(len(keys)):
                                if keys[j] >= i.value:
                                    tmp_list.append(pointers[j])
                            page_id = pointers[-1]
                            keys, pointers = im.get_leaf_info(index_id, page_id)                        
                        if loc_list == []:
                            loc_list = tmp_list
                        else:
                            loc_list = list(set(loc_list).intersection(tmp_list))
                            if loc_list == []:
                                break
                    
                    elif i.compare == "<":
                        ternimate = False
                        tmp_list = []
                        keys, pointers = im.get_min_info(index_id)
                        # 针对这是一张空表的情况
                        if keys == []:
                            if attribute == None:
                                return [self.title]
                            else:
                                return ([self.title], attribute)
                        for j in range(len(keys)):
                            if keys[j] < i.value:
                                tmp_list.append(pointers[j])
                        if pointers[-1] == None:
                            page_id = 0
                        elif type(pointers[-1]) is not int:
                            page_id = pointers[-1].page_id
                        else:
                            page_id = pointers[-1]

                        keys, pointers = im.get_leaf_info(index_id, page_id)
                        while keys != []:
                            for j in range(len(keys)):
                                if keys[j] < i.value:
                                    tmp_list.append(pointers[j])
                                else:
                                    ternimate = True
                            if ternimate:
                                break
                            page_id = pointers[-1]
                            keys, pointers = im.get_leaf_info(index_id, page_id)

                        if loc_list == []:
                            loc_list = tmp_list
                        else:
                            loc_list = list(set(loc_list).intersection(tmp_list))
                            if loc_list == []:
                                break
                    
                    elif i.compare == "<=":
                        tmp_list = []
                        keys, pointers = im.get_min_info(index_id)
                        # 针对这是一张空表的情况
                        if keys == []:
                            if attribute == None:
                                return [self.title]
                            else:
                                return ([self.title], attribute)
                        for j in range(len(keys)):
                            if keys[j] <= i.value:
                                tmp_list.append(pointers[j])
                        if pointers[-1] == None:
                            page_id = 0
                        elif type(pointers[-1]) is not int:
                            page_id = pointers[-1].page_id
                        else:
                            page_id = pointers[-1]
                        keys, pointers = im.get_leaf_info(index_id, page_id)
                        while keys != []:
                            for j in range(len(keys)):
                                if keys[j] <= i.value:
                                    tmp_list.append(pointers[j])
                            page_id = pointers[-1]
                            keys, pointers = im.get_leaf_info(index_id, page_id)                        
                        if loc_list == []:
                            loc_list = tmp_list
                        else:
                            loc_list = list(set(loc_list).intersection(tmp_list))
                            if loc_list == []:
                                break
                    
                    elif i.compare == "<>" or i.compare == "!=":
                        tmp_list = []
                        keys, pointers = im.get_min_info(index_id)
                        # 针对这是一张空表的情况
                        if keys == []:
                            if attribute == None:
                                return [self.title]
                            else:
                                return ([self.title], attribute)
                        for j in range(len(keys)):
                            if keys[j] != i.value:
                                tmp_list.append(pointers[j])
                        if pointers[-1] == None:
                            page_id = 0
                        elif type(pointers[-1]) is not int:
                            page_id = pointers[-1].page_id
                        else:
                            page_id = pointers[-1]
                        keys, pointers = im.get_leaf_info(index_id, page_id)
                        while keys != []:
                            for j in range(len(keys)):
                                if keys[j] != i.value:
                                    tmp_list.append(pointers[j])
                            page_id = pointers[-1]
                            keys, pointers = im.get_leaf_info(index_id, page_id)                        
                        if loc_list == []:
                            loc_list = tmp_list
                        else:
                            loc_list = list(set(loc_list).intersection(tmp_list))
                            if loc_list == []:
                                break
                    condition.remove(i)
        # 没有利用到索引，只能全表遍历，先按照主键进行一次排序
        if flag == False:
            # [Field, Type, begin, begin + Length, Prikey, Unique, Index_id]
            keys, pointers = im.get_min_info(self.primary_index)
            # 针对这是一张空表的情况
            if keys == []:
                if attribute == None:
                    return [self.title]
                else:
                    return ([self.title], attribute)
            for i in range(len(keys)):
                loc_list.append(pointers[i])
            if pointers[-1] == None:
                page_id = 0
            elif type(pointers[-1]) is not int:
                page_id = pointers[-1].page_id
            else:
                page_id = pointers[-1]
            keys, pointers = im.get_leaf_info(self.primary_index, page_id)
            while keys != []:
                print(keys, pointers)
                for i in range(len(keys)):
                    loc_list.append(pointers[i])
                page_id = pointers[-1]
                keys, pointers = im.get_leaf_info(self.primary_index, page_id)
        
        while (0,0) in loc_list:
            loc_list.remove((0,0))
        
        vTable = []
        for pid, rid in loc_list:
            vTable += self.get_record(pid, rid)
        # 进行遍历查找剩余没有建主键的属性的约束条件
        vTable = self._filter_data(vTable, condition)
        if attribute == None:
            return [self.title] + vTable
        else:
            return ([self.title] + vTable, attribute)

    def delete(self, condition):
        result = self.select(None, condition)
        # 删除记录并调整索引
        for i in range(1, len(result)):
            page_id = result[i][-2]
            if self.header.first_free_page == 0 or self.header.first_free_page > page_id:
                self.header.first_free_page = page_id
                bf.make_file_dirty(self.file_name)

            record_id = result[i][-1]
            page_data = bf.get_pagedata(self.file_name, page_id)
            if page_data.first_free_record == -1 or page_data.first_free_record == -1 > record_id:
                page_data.first_free_record = record_id
            bf.mark_record_invalid(self.file_name, page_id, record_id, self.record_length)
            
            # 调整索引
            for j in range(self.attribute_num):
                #  [Field, Type, begin, begin + Length, Prikey, Unique, Index_id]
                index_id = self.attr_dict[j][-1]
                if index_id != 0:
                    im.delete(index_id, result[i][j])
        return len(result) - 1
    
    def set_index(self, index_name, index_id, attr_name):
        idx = self.name_to_idx[attr_name]
        info = self.attr_dict[idx]
        attr_type = info[1]
        attr_length = info[3] - info[2]
        info[-1] = index_id
        self.header.data[272+(idx+1)*self.attribut_size-4:272+(idx+1)*self.attribut_size] = int_to_byte(index_id)
        bf.make_file_dirty(self.file_name)
        im.create_index(index_name, index_id, self.table_name, attr_name, attr_type, attr_length)
        # 扫描全表, 并将已存在的记录插入索引文件中
        records = []
        for i in range(1, self.header.size + 1):
            records += self._scan_page(i)
        for i in records:
            im.insert(index_id, i[idx], i[-2], i[-1])
        return 1
    
    def remove_index(self, attr_name):
        idx = self.name_to_idx[attr_name]
        self.attr_dict[idx][-1] = 0
        self.header.data[272+(idx+1)*self.attribut_size-4:272+(idx+1)*self.attribut_size] = int_to_byte(0)
        bf.make_file_dirty(self.file_name)
        return 1

    def _filter_data(self, source_table, condition):
        """
        从source_table找出符合条件的记录
        :param: source_table    [[]] 原始表
        :param: condition       Condition[] 多个比较条件
        """
        if(condition == None):
            return source_table
        result = []
        for i in source_table:
            valid = True
            for j in condition:
                idx = self.name_to_idx[j.attr_name]
                item = i[idx]
                valid &= self._judge_condition(item, j)
            if(valid):
                result.append(i)
        return result

    def _judge_condition(self, value, condition):
        if condition.compare == "=":
            if type(value) == float:
              return math.isclose(value, condition.value, rel_tol=1e-06)
            else:
                return value == condition.value
        elif condition.compare == ">":
          return value > condition.value
        elif condition.compare == ">=":
          return value >= condition.value
        elif condition.compare == "<":
          return value < condition.value
        elif condition.compare == "<=":
          return value <= condition.value
        elif condition.compare == "<>" or condition.compare == "!=":
          return value != condition.value
    
    def get_record(self, page_id, record_id):
        page_data =  bf.get_pagedata(self.file_name, page_id)
        offset = record_id * self.record_length
        byte_data = page_data.data[offset:offset+self.record_length]
        # 判断是否有被删除的mark
        if byte_to_bool(page_data.data[offset:offset+4]) == True:
                byte_data = page_data.data[offset:offset+self.record_length]
                record = self._scan_record(byte_data)
                return [record + [page_id, record_id]]
        return []

    def _scan_page(self, page_id):
        """
        返回嵌套列表
        :param page_id  int 
        return
        [
        [1, "test1", 1, 0],
        [2, "test2", 1, 1]
        ]
        """
        records = []
        page_data =  bf.get_pagedata(self.file_name, page_id)
        for rid in range(self.record_capacity):
            offset = rid * self.record_length
            # 判断是否有被删除的mark
            if byte_to_bool(page_data.data[offset:offset+4]) == True:
                byte_data = page_data.data[offset:offset+self.record_length]
                record = self._scan_record(byte_data)
                records.append(record+[page_id, rid])
        if debug:
            print(records)
        return records

    def _scan_record(self, byte_data):
        record = []
        for i in range(self.attribute_num):
        #  [Field, Type, begin, begin + Length, Prikey, Unique, Index_id]
            type = self.attr_dict[i][1]
            begin = self.attr_dict[i][2]
            end = self.attr_dict[i][3]
            record_byte = byte_data[begin : end]
            value = byte_to_key(type, record_byte)    
            record.append(value)
        return record

    def _scan_attr(self, record):
            """
            接受116字节的数据，转换为属性列表
            :param: record byte[]
            return 属性信息的截取，只需要属性名，属性长度和属性是否为unique
            """
            Field = byte_to_str(record[0:96])
            Type  = byte_to_int(record[96:100])
            Length = byte_to_int(record[100:104])
            Primary_key = byte_to_bool(record[104:108])
            Unique = byte_to_bool(record[108:112])
            Index_id = byte_to_int(record[112:116])
            return Field, Type, Length, Primary_key, Unique,  Index_id

class RecordManager:
    """
    提供一个不需要创建类对象调用的接口
    调用方式与CatalogManager基本相同
    实际上每张表对应的对象由RecordManager管控
    table_dict 字典
    table_name : 对应TableManager的实例
    """
    table_dict = {}
    @classmethod
    def create_table(cls, table_name, attr_rawdata, length):
        """
        在物理上创建文件（create table的表格一定是没有存在过的）
        :param table_name:      string 表名
        :param attr_rawdata:    
        bytearray 本表的属性信息, 与CatalogManager中保持一致，冗余设计
        :param length:          int 记录长度，代表该表的每个tuple需要多少byte去存放 
        """
        name = str_to_byte(table_name, 256)
        record_length = int_to_byte(length)
        record_capacity = int_to_byte(int(4092/length))
        header_data = bytearray(4084 * b'\x00')
        data = bytearray(name + record_length + record_capacity + attr_rawdata)
        header_data[0: len(data)] =  data
        header = PagedFileHeader(0, MAX_CAPACITY, 0, header_data)
        bf.create_file(table_name + "_record.db", header)
        cls.table_dict[table_name] = TableManager(table_name)
        return True

    @classmethod
    def destroy_table(cls, table_name):
        """
        丢弃所有更改,并在buffer manager里关闭文件流和移除所有属于该文件的缓冲块
        """
        bf.remove_file(table_name + "_record.db")
        return True

    @classmethod
    def select(cls, table_name, attr, condition):
        if table_name not in cls.table_dict.keys():
            cls.table_dict[table_name] = TableManager(table_name)
        return cls.table_dict[table_name].select(attr, condition)
    
    @classmethod
    def insert(cls, table_name, values):
        """
        对于某张表插入值
        :param values bytearray[] 已经由API整理好的二进制数据
        """
        if table_name not in cls.table_dict.keys():
            cls.table_dict[table_name] = TableManager(table_name)
        cls.table_dict[table_name].insert(values)
        return True

    @classmethod
    def delete(cls, table_name, condition):
        """
        对于某张表删除一些记录
        :param table_name   string 表名
        :param condition    Condition[] 条件列表
        """
        if table_name not in cls.table_dict.keys():
            cls.table_dict[table_name] = TableManager(table_name)
        return cls.table_dict[table_name].delete(condition)

    @classmethod
    def set_index(cls, index_name, index_id, table_name, attr_name):
        """
        对于某张表的某个属性设置索引id
        """
        if table_name not in cls.table_dict.keys():
            cls.table_dict[table_name] = TableManager(table_name)
        return cls.table_dict[table_name].set_index(index_name, index_id, attr_name)

    @classmethod
    def remove_index(cls, table_name, attr_name):
        """
        移除某张表某个属性上的索引
        """
        if table_name not in cls.table_dict.keys():
            cls.table_dict[table_name] = TableManager(table_name)
        return cls.table_dict[table_name].remove_index(attr_name)