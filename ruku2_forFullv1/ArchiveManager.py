
#Description
#Archive管理器
#功能1. 插入一条记录。insert_archive_with_details。
# 比如1景数据包，包含20个文件，需要入库。那么只需要整理好数据，使用功能1，一次会填写好两张表




class ArchiveManager:
    def __init__(self, db_inserter):
        self.db_inserter = db_inserter

    def insert_archive_with_details(self, archive_data, detail_data_list):
        # 插入 product_archive 数据并获取主键 ID
        archive_id = self.db_inserter.insert_data('product_archive', archive_data)

        # 为每个 detail 数据插入 product_archive_detail，使用获取的 archive_id
        for detail_data in detail_data_list:
            detail_data['archive_id'] = archive_id  # 将归档 ID 添加到详细信息中
            self.db_inserter.insert_data('product_archive_detail', detail_data)


    def fakedelete_archive_byProductID(self, basic_product_id):
        return

    def realdelete_archive_byProductID(self, basic_product_id):
        records_product_archive = self.db_inserter.fetch_by_field('product_archive', 'basic_product_id', basic_product_id)  #获取结构体
        result_file_list = []

        for i_archive in records_product_archive:   #遍历所有具有product_id的文件
            archive_id_int = int(i_archive['archive_id'])  #获取archive ID
            print('清除文件id:'+str(archive_id_int))
            archive_filepath = i_archive['file_path']  # 获取文件路径
            #获取文件信息
            records_archive_detail = self.db_inserter.fetch_by_field('product_archive_detail', 'archive_id',
                                                                      archive_id_int)  # 获取结构体
            for i_detail in records_archive_detail:
                detail_name = i_detail['file_path']
                result_file_list.append(archive_filepath + '/' + detail_name)   #获取文件Minio全路径
            #删除archive detail记录
            self.db_inserter.delete_by_field('product_archive_detail', 'archive_id', archive_id_int)
        #现在，删除所有archive记录
        self.db_inserter.delete_by_field('product_archive', 'basic_product_id', basic_product_id)
        return result_file_list





