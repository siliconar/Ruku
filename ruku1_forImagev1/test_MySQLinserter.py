from MySQLInserter import MySQLInserter


#####第一种测试,插入测试

if __name__ == "__main__":
    db_inserter = MySQLInserter(
        host='10.110.31.155',
        user='root',
        password='sitp@nais@22611',
        database='hyspec_dev'
    )

    data = {
        'file_id': 12,
        'archive_id': 113,
        'file_path': 'file123',
        'size': '2045'
    }

    db_inserter.insert_data('product_archive_detail', data)
    db_inserter.close()

#####第二种测试，测试更新与报错是否正常
# if __name__ == "__main__":
#     db_inserter = MySQLInserter(
#                 host='10.110.31.155',
#                 user='root',
#                 password='sitp@nais@22611',
#                 database='hyspec_dev'
#     )
#
#     # 示例：检查某个fileID是否存在
#     exists = db_inserter.check_field_exists('product_archive_detail', 'archive_id', 1)
#     print(f"fileID 1 exists: {exists}")



    # 示例：更新记录，但记录不唯一，应该报错
    # new_data = {
    #     'file_path': 'Updated path',
    #     'size': 'updated size'
    # }
    # db_inserter.update_by_field('product_archive_detail', 'archive_id', 1, new_data)


    # # 示例：更新记录，且记录唯一，正常更新
    # new_data = {
    #     'file_name': 'Updated name',
    #     'size': 'updated size'
    # }
    # db_inserter.update_by_field('product_archive', 'basic_product_id', 123, new_data)

    # 示例：删除某个fileID的记录
    # db_inserter.delete_by_field('your_table', 'fileID', 3)
    db_inserter.close()