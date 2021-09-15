from ast import Param
import queue
import re
import copy
import random
import os
from typing import KeysView
from Utils.Utils import *
from Global.DataExchange import *
from OtherException.Exception import *
from BufferManager.BufferManager import BufferManager as bf
from queue import Queue

debug = False

class TNode:
    def __init__(self, parent = None, size = None, isLeaf = None, keys = None, pointers = None, page_id = None):
        """
        通过缓冲区的一个块重建或者是通过构造函数获得
        :param parent 父节点
        :param size 当前含有的key的数量
        :param isLeaf 是否为根节点
        :param keys [] 键值
        :param pointers [] 子节点指针
        Note: pointers的数量总是比keys多一个
        """
        self.page_id = page_id
        self.parent = parent
        self.size = size
        self.isLeaf = isLeaf
        self.parent = parent
        self.keys = keys
        self.pointers = pointers


class BTree:
    def __init__(self, file_name = None):
        self.file_name = file_name
        self.header = bf.get_file_header(file_name)
        self.attr_type = byte_to_int(self.header.data[612:616])
        self.attr_length = byte_to_int(self.header.data[616:620])
        self.order = byte_to_int(self.header.data[620:624])
        self.root_id =  byte_to_int(self.header.data[624:628])
        if self.root_id == 0:
            self.root = None
        else:
            self.root = self.recover_from_buf(None, self.root_id)
        if debug:
            print("attr_type:", self.attr_type)
            print("attr_len:", self.attr_length)
            print("order:", self.order)
            print("root_id:", self.root_id)
        self.small_num = int((self.order+1)/2)
        self.big_num = self.order + 1 - self.small_num

    def recover_from_buf(self, parent, page_id):
        """
        从缓冲区的一页重建一个node
        """
        page_data = bf.get_pagedata(self.file_name, page_id)
        data = page_data.data
        isLeaf = byte_to_bool(data[0:4])
        size = byte_to_int(data[8:12])
        pointers = []
        keys = []
        for i in range(size):
            offset = i*8 + 12
            if isLeaf:
                pid = byte_to_int(data[offset:offset+4])
                rid = byte_to_int(data[offset+4:offset+8])
                pointers.append((pid, rid))
            else:
                pid = byte_to_int(data[offset:offset+4])
                pointers.append(pid)
        offset = size * 8 + 12
        pid = byte_to_int(data[offset:offset+4])
        if pid == 0:
            pointers.append(None)
        else:
            pointers.append(pid)

        for i in range(size):
            offset = size * 8 + 16 + i * self.attr_length
            keys.append(byte_to_key(self.attr_type, data[offset: offset + self.attr_length]))
        if debug:
            print("Page_id", page_id)
            print("isLeaf:", isLeaf)
            print("Size:", size)
            print("Pointers:", pointers)
            print("keys:", keys)
        return TNode(parent, size, isLeaf, keys, pointers, page_id)
    
    def suspend_to_buf(self, t):
        """
        将节点保存在缓冲区
        每一个节点对应一个页
        """
        data = bytearray(4092*b'\x00')
        data[0:4] = bool_to_byte(t.isLeaf)
        if t.parent == None:
            parent_id = 0
        else:
            parent_id = t.parent.page_id
        data[4:8] = int_to_byte(parent_id)
        data[8:12] = int_to_byte(t.size)
        for i in range(t.size):
            if t.isLeaf:
                pid, rid = t.pointers[i]
            else:
                rid = 0
                if type(t.pointers[i]) is int:
                    pid = t.pointers[i]
                else:
                    pid = t.pointers[i].page_id
            data[i*8 + 12: (i+1)*8 + 12] = int_to_byte(pid) + int_to_byte(rid)
        
        # 最后一个指针单独调整，仅用4 bytes去存放, 因为无论是leaf还是noneleaf的节点，都只需要page_id一个信息
        if type(t.pointers[t.size]) is int:
            pid = t.pointers[t.size]
        elif t.pointers[t.size] == None:
            pid = 0
        else:
            pid = t.pointers[t.size].page_id
        data[t.size*8 + 12: t.size*8 + 16] = int_to_byte(pid)
        
        key_begin = t.size*8 + 16
        for i in range(t.size):
            data[key_begin + i*self.attr_length: key_begin + (i+1)*self.attr_length ] = key_to_byte(self.attr_type, t.keys[i], self.attr_length)
        page_data = bf.get_pagedata(self.file_name, t.page_id)
        page_data.first_free_record = -1
        page_data.data = data
        bf.make_block_dirty(self.file_name, t.page_id)

    def get_leaf_info(self, page_id):
        """
        对外部的接口, 返回keys和pointers
        """
        if page_id == 0:
            return [], []
        else:
            page_data = bf.get_pagedata(self.file_name, page_id)
            data = page_data.data
            size = byte_to_int(data[8:12])
            pointers = []
            keys = []

            for i in range(size):
                offset = i*8 + 12
                pid = byte_to_int(data[offset:offset+4])
                rid = byte_to_int(data[offset+4:offset+8])
                pointers.append((pid, rid))
            offset = size * 8 + 12
            pid = byte_to_int(data[offset:offset+4])
            pointers.append(pid)
            for i in range(size):
                offset = size * 8 + 16 + i * self.attr_length
                keys.append(byte_to_key(self.attr_type, data[offset: offset + self.attr_length]))
            return keys, pointers

    def get_min(self):
        """
        对外部的接口,找到最小的node
        """
        t = self.root
        if t == None:
            return None
        while t.isLeaf == False:
            if type(t.pointers[0]) is int:
                t.pointers[0] = self.recover_from_buf(t, t.pointers[0])
            t = t.pointers[0]
        return t

    def search(self, key, t = None):
        """
        对外部的接口,找到key对应的pid和rid
        """
        if t == None:
            t = self.root
        if t == None:
            return None
        if t.isLeaf:
            for i in range(t.size):
                if t.keys[i] == key:
                    return t.pointers[i]
            return None
        idx = self.get_key_index(t, key)
        if type(t.pointers[idx]) is int:
            t.pointers[idx] = self.recover_from_buf(t, t.pointers[idx])
        return self.search(key, t.pointers[idx])    

    def print(self):
        """
        打印自身
        """
        q = Queue()
        q.put(self.root)
        size = 1
        next_size = 0
        count = 0
        while q.empty() == False:
            t = q.get()
            if(not t.isLeaf):
                next_size += t.size + 1
            print("[", end = '')
            for i in t.keys:
                print(i, end = ' ')
            print("]", end = '')
            if not t.isLeaf:
                for i in t.pointers:
                    q.put(i)
            count += 1
            if count == size:
                print('')
                size = next_size
                next_size = count = 0
        print("")

    def insert(self, key, page_id = 0, record_id = 0):
        """
        插入一条记录
        """
        if self.root == None:
            page_id = bf.allocate_available_page(self.file_name)
            self.header.first_free_page = 0
            self.header.data[624:628] = int_to_byte(page_id)
            self.root = TNode(  None, 
                                1, 
                                True, 
                                [key], 
                                [(page_id, record_id), None], 
                                page_id)
            self.suspend_to_buf(self.root)
        else:
            t = self.find(self.root, key)
            if t == None:
                """
                本条记录需要更新
                """
                t, idx = self.find_node(self.root, key)
                t.pointers[idx] = (page_id, record_id)
                self.suspend_to_buf(t)
                return

            if t.size != self.order:
                # 不需要分裂
                idx = self.get_key_index(t, key)
                t.keys.insert(idx, key)
                t.pointers.insert(idx, (page_id, record_id))
                t.size += 1
                self.suspend_to_buf(t)
            else:
                # 需要分裂
                idx = self.get_key_index(t, key)
                t.keys.insert(idx, key)
                t.pointers.insert(idx, (page_id, record_id))

                page_id = bf.allocate_available_page(self.file_name)
                self.header.first_free_page = 0
                big = TNode(t.parent, 
                            self.big_num, 
                            True, 
                            t.keys[self.small_num:], 
                            t.pointers[self.small_num:], 
                            page_id)
                t.size = self.small_num
                t.keys = t.keys[0: self.small_num]
                t.pointers = t.pointers[0: self.small_num]
                t.pointers.append(big)

                parent = None
                current = t
                parent_l = None
                parent_r = None
                while current != None:
                    parent = current.parent
                    if current == self.root:
                        # 当前节点就是根节点, 一分为2
                        page_id = bf.allocate_available_page(self.file_name)
                        self.header.first_free_page = 0
                        self.header.data[624:628] = int_to_byte(page_id)
                        newroot = TNode(None, 
                                        1, 
                                        False, 
                                        [self.find_min(big)], 
                                        [t, big], 
                                        page_id)
                        t.parent = newroot
                        big.parent = newroot
                        self.root = newroot
                        self.root_id = page_id
                        self.suspend_to_buf(newroot)
                        self.suspend_to_buf(t)
                        self.suspend_to_buf(big)
                        break
                    elif parent.size < self.order:
                        # 当前节点的父节点不满
                        idx = self.get_key_index(parent, key)
                        parent.pointers[idx] = t
                        parent.pointers.insert(idx + 1, big)
                        t.parent = parent
                        big.parent = parent
                        parent.keys.insert(idx, self.find_min(parent.pointers[idx + 1]))
                        parent.size += 1
                        self.suspend_to_buf(parent)
                        self.suspend_to_buf(t)
                        self.suspend_to_buf(big)
                        break
                    elif parent.size == self.order:
                        # 当前父节点已经满了
                        idx = self.get_key_index(parent, key)
                        parent.pointers[idx] = t
                        parent.pointers.insert(idx + 1, big)
                        parent.keys.insert(idx, self.find_min(parent.pointers[idx + 1]))
                        page_id = bf.allocate_available_page(self.file_name)
                        self.header.first_free_page = 0
                        parent_r =  TNode(
                                    current.parent, 
                                    self.big_num - 1, False, 
                                    parent.keys[self.small_num+1:], 
                                    parent.pointers[self.small_num+1:],
                                    page_id)
                        for i in parent.pointers[self.small_num+1:]:
                            i.parent =  parent_r
                        parent_l = parent
                        parent_l.size = self.small_num
                        parent_l.isLeaf = False
                        parent_l.keys = parent_l.keys[0:self.small_num]
                        parent_l.pointers = parent_l.pointers[0:self.small_num + 1]
                        t = parent_l
                        big = parent_r
                        self.suspend_to_buf(parent_l)
                        self.suspend_to_buf(parent_r)
                        self.suspend_to_buf(t)
                        self.suspend_to_buf(big)
                        current = parent


    def get_key_index(self, t, key):
        """
        找到待插入的key在keys里面应该存在的位置
        """
        idx = 0
        while(idx < t.size and key >= t.keys[idx]):
            idx += 1
        return idx

    def find(self, t, key):
        """
        如果有重复的,则返回None
        否则，返回待插入节点所应在的子节点
        """
        if t.isLeaf:
            for i in range(t.size):
                if t.keys[i] == key:
                    return None
            return t
        idx = self.get_key_index(t, key)
        return self.find(t.pointers[idx], key)
    
    def find_node(self, t, key):
        if t.isLeaf:
            for i in range(t.size):
                if t.keys[i] == key:
                    return t, i
        idx = self.get_key_index(t, key)
        if type(t.pointers[idx]) is int:
            t.pointers[idx] = self.recover_from_buf(t, t.pointers[idx])
        return self.find_node(t.pointers[idx], key)

    def find_min(self, t):
        """
        找出t和t的子树中最小的key
        """
        while t.isLeaf == False:
            t = t.pointers[0]
        return t.keys[0] 
    

    def delete(self, key):
        """
        对于叶子节点，记录的位置和key的按照下标关系一一对应
        """
        t, i = self.find_node(self.root, key)
        t.pointers[i] = (0, 0)
        self.suspend_to_buf(t)
        return True
    
class IndexManager:
    """
    提供一个不需要创建类对象调用的接口
    调用方式与RecordManager基本相同
    实际上每张表对应的对象由BTree管控
    index_dict 字典
    index_name: 对应BTree的实例
    """
    index_dict = {}
    @classmethod
    def create_index(cls, index_name, index_id, table_name, attr_name, attr_type, attr_length):
        file_name = index_name + "_" + str(index_id) + ".idx"
        data = bytearray(4084 * b'\x00')
        data[0:256] = str_to_byte(table_name, 256)
        data[256:260] = int_to_byte(index_id)
        data[260:516] = str_to_byte(index_name, 256)
        data[516:612] = str_to_byte(attr_name, 96)
        data[612:616] = int_to_byte(attr_type)
        data[616:620] = int_to_byte(attr_length)
        # isLeaf 4 bytes, parent 4 bytes, size 4 bytes, extra ptr 4 bytes
        order = int((4092 - 4 - 4 - 4 - 4) / (8 + attr_length))
        # order
        data[620:624] = int_to_byte(order)
        # root
        data[624:628] = int_to_byte(0)
        header = PagedFileHeader(0, MAX_CAPACITY, 0, data)
        bf.create_file(file_name, header)
        cls.index_dict[index_id] = BTree(file_name)
    
    @classmethod
    def get_new_index_id(cls):
        count = 1
        f_list = os.listdir(DBFILES_FOLDER)
        for i in f_list:
        # os.path.splitext():分离文件名与扩展名
            if os.path.splitext(i)[1] == '.idx':
                count += 1
        return count
    
    @classmethod
    def get_file_name(cls, index_id):
        f_list = os.listdir(DBFILES_FOLDER)
        pattern = str(index_id) + '.idx'
        for i in f_list:
            if i.find(pattern) != -1:
                return i
        return None

    @classmethod
    def get_info_by_index_name(cls, index_name):
        f_list = os.listdir(DBFILES_FOLDER)
        pattern = index_name + '_'
        flag = False
        for i in f_list:
            if i.find(pattern) == 0:
                flag = True
                break
        if flag == False:
            return None, None
        header = bf.get_file_header(i)
        data = header.data
        table_name = byte_to_str(data[0:256])
        attr_name = byte_to_str(data[516:612])
        return table_name, attr_name

    @classmethod
    def insert(cls, index_id, key, page_id, record_id):
        if index_id not in cls.index_dict.keys():
            file_name = cls.get_file_name(index_id)
            cls.index_dict[index_id] = BTree(file_name)
        cls.index_dict[index_id].insert(key, page_id, record_id)

    @classmethod
    def search(cls, index_id, key):
        if index_id not in cls.index_dict.keys():
            file_name = cls.get_file_name(index_id)
            cls.index_dict[index_id] = BTree(file_name)
        return cls.index_dict[index_id].search(key)

    @classmethod
    def delete(cls, index_id, key):
        if index_id not in cls.index_dict.keys():
            file_name = cls.get_file_name(index_id)
            cls.index_dict[index_id] = BTree(file_name)
        return cls.index_dict[index_id].delete(key)
    
    @classmethod
    def get_min_info(cls, index_id):
        if index_id not in cls.index_dict.keys():
            file_name = cls.get_file_name(index_id)
            cls.index_dict[index_id] = BTree(file_name)
        t = cls.index_dict[index_id].get_min()
        if t == None:
            return [], []
        return t.keys, t.pointers
    
    @classmethod
    def get_leaf_info(cls, index_id, page_id):
        if index_id not in cls.index_dict.keys():
            file_name = cls.get_file_name(index_id)
            cls.index_dict[index_id] = BTree(file_name)
        keys, pointers = cls.index_dict[index_id].get_leaf_info(page_id)
        return keys, pointers
    
    @classmethod
    def remove_index(cls, index_name):
        f_list = os.listdir(DBFILES_FOLDER)
        pattern = index_name + '_'
        for i in f_list:
            if i.find(pattern) == 0:
                break
        bf.remove_file(i)
        ori = os.path.join(DBFILES_FOLDER, i)
        new = os.path.join(DBFILES_FOLDER, "!droped_"+i)
        os.rename(ori, new)
        cls.index_dict.clear()
    
    @classmethod
    def sync(cls):
        cls.index_dict.clear()
        
# if __name__ == '__main__':
#     t = BTree(4)
#     l = []
#     for i in range(0, 10000):
#         # key  = i
#         key = random.randint(1, 100000)
#         if key not in l:
#             t.insert(key)
#         l.append(key)
#         # print("Seq: ", i)
#         # t.print()
#     t.print()
#     min_node = t.get_min()
#     # # t.insert(12)
#     # # node = t.find_min(t.root)
#     # # node, i =  t.search(25)
#     # # print(node, i)
#     # # print(len(node.pointers), node.pointers)
#     node = min_node
#     while node != None:
#         for i in node.keys:
#             print(i,' ', end = '')
#         node = node.pointers[-1]