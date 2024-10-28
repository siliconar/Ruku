
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


