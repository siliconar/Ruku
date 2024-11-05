



# 遍历文件夹
# {
#     找到某景的文件夹
#     转换thumb图
#     读取描述文件
#     生成新的存放路径(可能不做)
#     上传minio
#     生成/获取minio路径
#
#     入库archive
#
#     入库product
#
#
#
# }


import os
from ImageResizer import  ImageResizer
import shutil   #用户拷贝
from MetaReader import  MetaReader  #meta读取与整理
from MinioClientWrapper import MinioClientWrapper  #minio操作类
from datetime import datetime
from MySQLInserter import MySQLInserter      #用于操作数据库
from ArchiveManager import ArchiveManager   #封装的，一次可以操作归档的2个表入库


folder_path = 'C:/Users/SITP/Desktop/好看'

####固定变量
file_translation_file = 'C:/Users/SITP/Desktop/sun04/Ruku/ruku1_forImagev1/translate_meta2label.csv'  # translation.csv 文件的路径

obj_thumb_resizer = ImageResizer()  # 用于生成thumb的类，详细可以参考test_thumb.py

barrel = "hyspec"  # minio中，bucket的名字
minio_client = MinioClientWrapper(
    endpoint="10.110.31.154:9000",
    access_key="minio_Q2xXTs",
    secret_key="minio_Jr48cB",
    secure=False  # 使用http
)  # 初始化 Minio 客户端


db_inserter = MySQLInserter(
    host='10.110.31.155',
    user='root',
    password='sitp@nais@22611',
    database='hyspec_dev'
)  # 生成一个mysqt注入器，注意这个要close
archive_manager = ArchiveManager(db_inserter)  #生成archive打包器

##################
# 第一层，所有文件的总目录
for entry in os.listdir(folder_path):
    subfolder_path = os.path.join(folder_path, entry)
    # 将 Windows 路径分隔符替换为 `/`
    subfolder_path = subfolder_path.replace('\\', '/')
    # 检查是否为子文件夹
    if not os.path.isdir(subfolder_path):
        continue
    folder_sat_name = entry    # 只保留子文件夹名称
    folder_sat_fullpath = subfolder_path   # 子文件夹的完整路径
    print('1. 正在处理'+folder_sat_name+'')

    #第二层，处理某卫星
    for entry2 in os.listdir(folder_sat_fullpath):
        subfolder_path2 = os.path.join(folder_sat_fullpath, entry2)
        subfolder_path2 = subfolder_path2.replace('\\', '/')
        # 检查是否为子文件夹
        if not os.path.isdir(subfolder_path2):
            continue
        folder_date_name = entry2  # 只保留子文件夹名称
        folder_date_fullpath = subfolder_path2  # 子文件夹的完整路径
        print('1.1 正在处理' + folder_sat_name + ':' + folder_date_name)

        # 第3层，处理景
        for entry3 in os.listdir(folder_date_fullpath):
            subfolder_path3 = os.path.join(folder_date_fullpath, entry3)
            subfolder_path3 = subfolder_path3.replace('\\', '/')
            # 检查是否为子文件夹
            if not os.path.isdir(subfolder_path3):
                continue
            folder_sceneID = entry3  # 只保留子文件夹名称
            folder_scene_fullpath = subfolder_path3  # 子文件夹的完整路径
            print('1.1.1 正在处理' + folder_sat_name + ':' + folder_date_name + ':' + folder_sceneID)

            ## 处理文件
            file_browse_full = os.path.join(folder_scene_fullpath, 'pic.png')   #browse文件
            file_meta_full = os.path.join(folder_scene_fullpath, 'meta.txt')  # meta文件
            ## 判断文件是否存在
            if not os.path.exists(file_browse_full):
                raise ValueError(f"No File found with {file_browse_full}.")
            if not os.path.exists(file_meta_full):
                raise ValueError(f"No File found with {file_meta_full}.")
            ## 读取描述文件
            obj_meta_reader = MetaReader(file_meta_full, file_translation_file)     #类实例，用于提取meta信息，写成json格式
            data_meta = obj_meta_reader.get_translated_metadata()                   #meta数据
            ## 提取一些数据方便使用
            cur_SceneID_int = data_meta['xml_scene_id']                             #景号
            cur_ProductID_int = data_meta['basic_product_id']                       #产品号
            cur_SatID_str = data_meta['xml_satellite_id']                           #卫星ID
            cur_StartTm_dt = datetime.strptime(data_meta['xml_start_time'], "%Y-%m-%d %H:%M:%S")
            cur_year_str = str(cur_StartTm_dt.year)                                 # 年
            cur_month_str = cur_StartTm_dt.strftime("%m")  # 月
            cur_day_str = cur_StartTm_dt.strftime("%d")  # 日

            ## 安全检查，检查数据库中，该内容是否存在
            bexist_basic = db_inserter.check_field_exists('product_basic', 'basic_product_id', cur_ProductID_int)
            if bexist_basic:
                print('***FBI WARNING:***' + str(cur_ProductID_int)+'已经存在，无需入库')
                continue

            ## 生成thumb图
            file_thumb_full = os.path.join(folder_scene_fullpath, str(cur_ProductID_int)+'_'+str(cur_SceneID_int)+'_thumb.jpg')  # thumb文件
            obj_thumb_resizer.save_resized_image(file_browse_full,300,file_thumb_full)

            #拷贝browse，给browse图换个名字，因为一会上传minio方便
            file_browse2_full = os.path.join(folder_scene_fullpath, str(cur_ProductID_int)+'_'+str(cur_SceneID_int) + '.png')   #新的browse文件
            shutil.copy(file_browse_full, file_browse2_full)

            ## 生成本地新的存放路径(可能不做)
            ## 上传minio
            file_minio_basepath = 'satellite/' +cur_SatID_str+'/'+cur_year_str+'/'+cur_month_str+cur_day_str+'/'+str(cur_ProductID_int) #'/platform/G5A/年/月日/产品号'
            file_minio_browse2_full = file_minio_basepath+'/'+ os.path.basename(file_browse2_full)   # minio的browse文件全路径
            file_minio_thumb_full = file_minio_basepath + '/' + os.path.basename(file_thumb_full)  # minio的thumb文件全路径
            minio_client.upload_file(barrel, file_minio_browse2_full, file_browse2_full)    #上传
            minio_client.upload_file(barrel, file_minio_thumb_full, file_thumb_full)    #上传
            # print(file_minio_browse2_full)
            # print(file_minio_thumb_full)

            ## 入库archive
            archive_data = {
                'basic_product_id': cur_ProductID_int,
                'status': 1,
                'file_name': os.path.basename(file_minio_basepath),
                'file_path': file_minio_basepath,
                'is_deleted': 0,
                'reason': 'test02',
                'archive_time': datetime.now()
            }
            detail_data_list = [
                {'file_path': os.path.basename(file_minio_browse2_full), 'size': str(os.path.getsize(file_browse2_full)) },
                {'file_path': os.path.basename(file_minio_thumb_full), 'size': str(os.path.getsize(file_thumb_full)) }
            ]

            # 用archive打包器入库
            archive_manager.insert_archive_with_details(archive_data, detail_data_list)

            ## 入库product
            product_basic = {
                'basic_product_id': cur_ProductID_int,
                'is_valid': 1,
                'is_store': 0,  # 非完整数据
                'source': 'ZY',  #资源中心
                'is_archived': 1,
                'type': 'STANDARD',
                'platform': 'Satellite',
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
                'is_deleted': 0
            }


            #Geotiff大写
            data_meta['xml_product_format'] = data_meta['xml_product_format'].upper()
            # print(data_meta['xml_product_format'])
            # 'POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))'
            str_polygon = 'POLYGON((' + str(data_meta['xml_top_left_latitude']) + ' ' + \
                          str(data_meta['xml_top_left_longitude']) + ',' + \
                          str(data_meta['xml_top_right_latitude']) + ' ' + \
                          str(data_meta['xml_top_right_longitude']) + ',' + \
                          str(data_meta['xml_bottom_right_latitude']) + ' ' + \
                          str(data_meta['xml_bottom_right_longitude']) + ',' + \
                          str(data_meta['xml_bottom_left_latitude']) + ' ' + \
                          str(data_meta['xml_bottom_left_longitude']) + ',' + \
                          str(data_meta['xml_top_left_latitude']) + ' ' + \
                          str(data_meta['xml_top_left_longitude']) + '))'   #请注意，一个四边形，一定要5个点，最后一个点和第一个坐标相同,注意，一定要先lat后long
            # print('str_poly = '+str_polygon)

            product_detail_extra = {
                'thumb_url': file_minio_thumb_full,
                'preview_url': file_minio_browse2_full,
                'polygon':    str_polygon   #'POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))'
            }
            product_detail_combine = data_meta | product_detail_extra
            # mysql入库
            inserted_id2 = db_inserter.insert_data('product_basic', product_basic)
            inserted_id3 = db_inserter.insert_data('product_detail', product_detail_combine)


#####  最后清理环境

db_inserter.close()

#
#
# detail_data_list = []
#
# # 遍历文件夹中的文件
# for file_name in os.listdir(folder_path):
#     file_path = os.path.join(folder_path, file_name)
#
#     # 检查是否是文件
#     if os.path.isfile(file_path):
#         # 获取文件大小
#         file_size = os.path.getsize(file_path)
#
#         # 添加到detail_data_list
#         detail_data_list.append({
#             'file_path': file_name,  # 只保留文件名
#             'size': str(file_size)  # 文件大小
#         })




