from MySQLInserter import MySQLInserter
from ArchiveManager import ArchiveManager
from datetime import datetime

if __name__ == "__main__":
    db_inserter = MySQLInserter(
        host='10.110.31.155',
        user='root',
        password='sitp@nais@22611',
        database='hyspec_dev'
    )  #生成一个mysqt注入器

    archive_data = {
        'basic_product_id': 223,
        'status': 1,
        'file_name': '/file_name',
        'file_path': '/filepath',
        'is_deleted': 0,
        'reason': 'test01',
        'archive_time': datetime.now()
    }

    detail_data_list = [
        {'file_path': 'file21.txt', 'size': '123'},
        {'file_path': 'file22.txt', 'size': '223'},
        {'file_path': 'file23.txt', 'size': '323'}
    ]

    archive_manager = ArchiveManager(db_inserter)
    archive_manager.insert_archive_with_details(archive_data, detail_data_list)