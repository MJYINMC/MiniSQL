from pickle import TRUE
from Utils.Utils import *
from Global.DataExchange import *
from OtherException.Exception import *
import os

debug = True

# 本文件封装的文件读写操作需要的类，不与外部模块共享
class FileInfo:
    def __init__(self, file_name, header, file_stream):
        """
        初始化函数
        :param file_name: string
        :param header:    PagedFileHeader
        :param file:      stream
        dirty:            bool
        """
        self.file_name = file_name
        self.header = header
        self.file_stream = file_stream
        # 文件头是否被改写过
        self.dirty = False

class BufferBlock:
    def __init__(self, file_name, page_data, page_id):
        """
        初始化函数
        file_name:   string，page所属的文件名
        page_id:     int，该页在文件中的相对位置，0为header页
        dirty:       bool，是否为脏数据（被修改过）
        pin_count:   int，pin计数，为0时才能被flush掉
        page_data:   PageData
        """
        self.file_name = file_name
        self.page_id = page_id
        self.dirty = False
        self.pin_count = 0
        self.page_data = page_data

class BufferManager:
    # capacity:           int，缓冲区块数容量
    # size:               int，现有的缓冲块数量
    # buffer_block_array: BufferBlock[]，存放缓冲块的数组
    # file_info_array:    FileInfo[]，存放文件信息的数组（文件名、文件头、文件流、文件头是否修改过），每次涉及文件操作时需要及时维护
    capacity = 200
    size = 0
    buffer_block_array = []
    file_info_array = []

    # 外部接口
    @classmethod
    def make_file_dirty(cls, file_name):
        """
        将某file_info标记为dirty
        :param filename:    string 文件名
        """
        cls._read_file_into_list(file_name)
        file_info = cls._get_file_info(file_name)
        if(file_info != None):
            file_info.dirty = True

    @classmethod
    def make_block_dirty(cls, file_name, page_id):
        """
        将某block标记为dirty
        :param filename:    string 文件名
        :param page_id:     int    页数
        """
        cls._read_page_into_buf(file_name, page_id)
        block = cls._get_buffer_block(file_name, page_id)
        if(block != None):
            block.dirty = True

    @classmethod
    def mark_record_invalid(cls, file_name, page_id, record_id, record_length):
        """
        将某记录标记为标记为无效
        :param file_name: string
        :param page_id:   int
        """
        cls._read_page_into_buf(file_name, page_id)
        block = cls._get_buffer_block(file_name, page_id)
        cls._read_file_into_list(file_name)
        if block.page_data.first_free_record == -1:
            block.page_data.first_free_record = record_id
        elif block.page_data.first_free_record > record_id:
            block.page_data.first_free_record = record_id
        block.page_data.data[record_id * record_length : 4 + record_id * record_length] = bool_to_byte(False)
        block.dirty = True

    @classmethod
    def set_page(cls, file_name, page_id, page_data):
        cls._read_page_into_buf(file_name, page_id)
        block = cls._get_buffer_block(file_name, page_id)
        block.page_data = page_data
        block.dirty = True

    @classmethod
    def get_file_header(cls, file_name):
        """
        取得头文件，若不在缓冲区内则需要添加
        :param file_name 文件名
        return PagedFileHeader
        """
        cls._read_file_into_list(file_name)
        return cls._get_file_info(file_name).header

    @classmethod
    def get_pagedata(cls, file_name, page_id):
        """
        取得页数据
        :param file_name:   string
        :param page_id:     int
        return PageData
        """
        cls._read_page_into_buf(file_name, page_id)
        return cls._get_buffer_block(file_name, page_id).page_data
    
    @classmethod
    def create_file(cls, file_name, header):
        """
        实际上就是创建一个新的文件, 并写入header, 否则roll back会出错
        :param file_name: string
        :param header:    PagedFileHeader
        """
        cls.remove_file(file_name)
        # 创建文件
        file = open(os.path.join(DBFILES_FOLDER, file_name), 'wb')
        file.close()
        """
        需要立即写入, 否则对于以下操作序列
        1. 不存在元数据文件, 对catalog进行查询
        2. 马上roll back
        3. 关闭或者commit
        会导致产生一个空的元数据文件，使得下一次打开时读取到无意义的'\x00'数据
        """
        file_stream = cls._get_file_stream(file_name)
        new_file = FileInfo(file_name, header, file_stream)
        new_file.dirty = False
        cls._write_header(new_file)
        cls.file_info_array.append(new_file)

    @classmethod
    def remove_file(cls, file_name):
        """
        删除一个文件，
        为了冗余设计, 暂时不删除该文件但清除缓冲区中所有相关的内容
        :param file_name string 文件名
        """
        for i in cls.buffer_block_array:
            if i.file_name == file_name:
                cls.buffer_block_array.remove(i)
                cls.size -= 1
        
        for i in cls.file_info_array:
            if i.file_name == file_name:
                i.file_stream.close()
                cls.file_info_array.remove(i)

        # 暂时不从物理层面删除文件, 用户可以将这个表文件拷贝并单独调用RecordManager来查看数据
        # os.remove(os.path.join(DBFILES_FOLDER, file_name))

    @classmethod
    def allocate_available_page(cls, file_name):
        """
        在没有可用页的情况下创建一个新页，若需要可用页请直接用header中的ffp的page_id
        :param file_name:   string  要写入的文件
        return:             int     page_id
        """
        file_info = cls._get_file_info(file_name)
        header = file_info.header
        data = bytearray(b'\x00' * 4092)
        new_page = PageData(0, data)
        if header.first_free_page == 0:
            # 创建一个新页
            header.size = header.size + 1
            page_id = header.size
            if(header.size > header.capacity):
                # 无法再插入新页，报出错误
                raise OutOfRange("Maximum data scale reached")
            # 惰性读写，将块读入到缓冲区
            block =  BufferBlock(file_name, new_page, page_id)
            block.dirty = True     
            if(cls.size < cls.capacity):
                cls.size += 1
                cls.buffer_block_array.append(block)
            else:
                # 换页之后size依然等于capacity，不需要更新
                for i in cls.buffer_block_array:
                    if cls._remove_block(i):
                        break;
                cls.buffer_block_array.append(block)
            header.first_free_page = page_id
            # 需要改写头文件，将文件信息置为脏
            file_info.dirty = True
        else:
            # 有可用页
            page_id = header.first_free_page
        return page_id

    @classmethod
    def flush(cls):
        count = 0
        for i in cls.buffer_block_array:
            if i.dirty:
                count += 1
                cls._write_page(i)
        print("%d block(s) upgraded" % (count))
        
        count = 0
        for i in cls.file_info_array:
            if i.dirty:
                count += 1
                cls._write_header(i)
        print("%d file(s) upgraded " % (count))

    @classmethod
    def roll_back(cls):
        cls.file_info_array.clear()
        cls.buffer_block_array.clear()

    # 内部函数
    @classmethod
    def _get_buffer_block(cls, file_name, page_id):
        """
        检查某文件的某一页是否已经被读入了buffer中，若有，则返回该BufferBlock
        否则，返回None
        :param file_name:   string 文件名
        :param page_id:     int    页数
        return BufferBlock
        """
        for i in range(cls.size-1, -1, -1):
            if cls.buffer_block_array[i].file_name == file_name and cls.buffer_block_array[i].page_id == page_id:
                return cls.buffer_block_array[i]
        return None
    
    @classmethod
    def _get_file_info(cls, file_name):
        """
        检查某文件是否存在于打开的列表中，若存在，则返回该FileInfo
        否则，返回None
        :param file_name:   string 文件名
        return FileInfo
        """
        for i in cls.file_info_array:
            if i.file_name == file_name:
                return i
        return None

    @classmethod
    def _get_file_stream(cls, file_name):
        """
        从文件名获得文件流，若已存在对应的流，则直接返回，否则使用open函数打开一个
        :param file_name: string
        """
        file_info = cls._get_file_info(file_name)
        if file_info != None:
            return file_info.file_stream
        # 以覆盖读写模式打开文件
        file_stream = open(os.path.join(DBFILES_FOLDER, file_name), 'rb+')
        return file_stream

    @classmethod
    def _read_file_into_list(cls, file_name):
        """
        首先检查文件是否存在当前列表中，若存在，则无需操作，否则读入到文件列表中
        :param file_name:   string 文件名
        """
        # 若已经被读取过，则不需要再次读取
        if cls._get_file_info(file_name):
            return
        # 获得文件句柄
        file_stream = cls._get_file_stream(file_name)
        header = cls._read_header(file_stream)
        cls.file_info_array.append(FileInfo(file_name, header, file_stream))
        
    @classmethod
    def _read_page_into_buf(cls, file_name, page_id):
        """
        首先该文件的该页是否已经在缓冲区内，若不存在，则读入缓冲区
        :param file_name: string
        :param page_id:   int
        """
        # 已在缓冲区内则不需要则再读入
        if cls._get_buffer_block(file_name, page_id) != None:
            return
        # 获得文件句柄
        file_info = cls._get_file_info(file_name)
        if  file_info != None:
            file_stream = file_info.file_stream
            header = file_info.header
        else:
            file_stream = cls._get_file_stream(file_name)
            header = cls._read_header(file_stream)
        
        if page_id > header.capacity:
            raise OutOfRange("Internal error, check if '%s' has been modified by other program" % file_name)
        else:
            page_data = cls._read_page(file_stream, page_id)
            block = BufferBlock(file_name, page_data, page_id)
            # 读入缓冲区操作，首先检查缓冲区是否已满
            if(cls.size < cls.capacity):
                cls.size += 1
                cls.buffer_block_array.append(block)
            else:
                # 换页之后size依然等于capacity，不需要更新
                for i in cls.buffer_block_array:
                    if cls._remove_block(i):
                        break;
                cls.buffer_block_array.append(block)
            cls.file_info_array.append(FileInfo(file_name, header, file_stream))

    @classmethod
    def _remove_block(cls, block):
        """
        清除某buffer_block（在非pin情况下）
        :param block    : BufferBlock
        return:           bool
        """
        if block.pin_count == 0:
            if block.dirty :
                cls._write_page(block)
            # cls._update_file_list(block)
            cls.buffer_block_array.remove(block)
            return True
        else:
            return False

    @classmethod
    def _update_file_list(cls, block):
        """
        每次删除或置换一个block前, 检查缓冲区中是否至少还有一个块和被删的块属于同一文件
        若没有则需要进行删除操作
        :param block 待删除的块:    BufferBlock
        """
        file_name = block.file_name
        count = 0
        for i in cls.buffer_block_array:
            if i.file_name == file_name:
                count = count + 1
        if count <= 1:
            for i in cls.file_info_array:
                if i.file_name == file_name:
                    if i.dirty:
                        cls._write_header(i)
                    cls.file_info_array.remove(i)
                    i.file_stream.close()
                    break

    @classmethod
    def _read_header(cls, file_stream):
        """
        从文件中读文件头，底层动作
        :param file_stram:  stream
        :return:            PagedFileHeader
        """
        file_stream.seek(0, 0)
        header_data = file_stream.read(PAGE_SIZE)
        first_free_page = byte_to_int(header_data[0:4])
        capacity = byte_to_int(header_data[4:8])
        size = byte_to_int(header_data[8:12])
        header_rawdata = bytearray(header_data[12:4096])
        return PagedFileHeader(first_free_page, capacity, size, header_rawdata)

    @classmethod
    def _write_header(cls, file_info):
        """
        根据文件名写回对应的头文件
        :param file_info: FileInfo
        """
        header = file_info.header
        file_stream = file_info.file_stream
        file_stream.seek(0, 0)
        if header.data is None:
            header.data = bytearray(b'\x00' * 4084)
        ffp = int_to_byte(header.first_free_page)
        hc = int_to_byte(header.capacity)
        hs = int_to_byte(header.size)
        file_stream.seek(0, 0)
        file_stream.write(ffp)
        file_stream.write(hc)
        file_stream.write(hs)
        file_stream.write(header.data)
        file_stream.flush()
        file_info.dirty = False

    @classmethod
    def _read_page(cls, file_stream, page_id):
        """
        从文件中读页，底层动作
        :param file:    stream
        :param page_id: int
        :return:        PageData
        """
        offset = page_id * PAGE_SIZE
        file_stream.seek(offset, 0)
        page_data = file_stream.read(PAGE_SIZE)
        first_free_record = byte_to_int(page_data[0:4])
        page_bytearray = bytearray(page_data[4:4096])
        return PageData(first_free_record, page_bytearray)

    @classmethod
    def _write_page(cls, block):
        """
        将脏页写回文件
        :param block    : BufferBlock 
        """
        file_name = block.file_name
        page_id = block.page_id
        page = block.page_data
        offset = page_id * PAGE_SIZE
        file_stream = cls._get_file_stream(file_name)
        if page.data is None:
            page.data = bytearray(b'\x00'*4092)
        page_data = int(page.first_free_record).to_bytes(length=4, byteorder='big', signed=True) + page.data
        file_stream.seek(offset, 0)
        file_stream.write(page_data)
        file_stream.flush()