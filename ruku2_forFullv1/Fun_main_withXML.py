







# 遍历文件夹
# {
#     判断文件是否齐全
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
from XMLReader import  XMLReader  #xml读取与整理
from MinioClientWrapper import MinioClientWrapper  #minio操作类
from datetime import datetime
from MySQLInserter import MySQLInserter      #用于操作数据库
from ArchiveManager import ArchiveManager   #封装的，一次可以操作归档的2个表入库

def fun_withXML(file_xml_full, db_inserter, obj_thumb_resizer,minio_client,barrel,archive_manager):
    ####固定变量
    file_translation_file = 'translate_xml2label.csv'  # translation.csv 文件的路径
    file_basepath = os.path.dirname(file_xml_full)  #基础文件夹路径
    file_basename_nosuffix = os.path.splitext(os.path.basename(file_xml_full))[0]  # 只获取文件名，没有路径,没有后缀


    ## 判断文件是否齐全
    file_SW_full = os.path.join(file_basepath, file_basename_nosuffix+'_SW.geotiff')
    file_VN_full = os.path.join(file_basepath, file_basename_nosuffix + '_VN.geotiff')
    file_jpg_full = os.path.join(file_basepath, file_basename_nosuffix + '.jpg')
    if not os.path.exists(file_SW_full):
        raise ValueError(f"No File found with {file_SW_full}.")
    if not os.path.exists(file_VN_full):
        raise ValueError(f"No File found with {file_VN_full}.")
    if not os.path.exists(file_jpg_full):
        raise ValueError(f"No File found with {file_jpg_full}.")
    ## 读取描述文件
    obj_xml_reader = XMLReader(file_xml_full, file_translation_file)  # 类实例，用于提取meta信息，写成json格式
    data_meta = obj_xml_reader.get_translated_metadata()  # meta数据
    ## 提取一些数据方便使用
    cur_SceneID_int = data_meta['xml_scene_id']  # 景号
    cur_ProductID_int = data_meta['basic_product_id']  # 产品号
    cur_SatID_str = data_meta['xml_satellite_id']  # 卫星ID
    cur_StartTm_dt = datetime.strptime(data_meta['xml_start_time'], "%Y-%m-%d %H:%M:%S")
    cur_year_str = str(cur_StartTm_dt.year)  # 年
    cur_month_str = cur_StartTm_dt.strftime("%m")  # 月
    cur_day_str = cur_StartTm_dt.strftime("%d")  # 日
    ## 安全检查，检查数据库中，该内容是否存在
    bexist_basic = db_inserter.check_field_exists('product_basic', 'basic_product_id', cur_ProductID_int)
    if bexist_basic:

        print('****Note: 原来的库有产品，现在删除记录，重新更新：productID:'+str(cur_ProductID_int))
        # 清理原来的archive表和archive_detail
        minio_file_list = archive_manager.realdelete_archive_byProductID(cur_ProductID_int)

        #清理原来的minio文件
        for minio_file in minio_file_list:
            minio_client.delete_file(barrel,minio_file)

        # 清理原来的product的2个表
        db_inserter.delete_by_field('product_basic', 'basic_product_id', cur_ProductID_int)
        db_inserter.delete_by_field('product_detail', 'basic_product_id', cur_ProductID_int)


    ## 生成thumb图
    file_thumb_full = os.path.join(file_basepath, file_basename_nosuffix +  '_thumb.jpg')  # thumb文件
    obj_thumb_resizer.save_resized_image(file_jpg_full, 300, file_thumb_full)

    ## 上传minio
    file_minio_basepath = 'satellite/' +cur_SatID_str+'/'+cur_year_str+'/'+cur_month_str+cur_day_str+'/'+str(cur_ProductID_int) #'/platform/G5A/年/月日/产品号'
    file_minio_browse2_full = file_minio_basepath+'/'+ os.path.basename(file_jpg_full)   # minio的browse文件全路径
    file_minio_thumb_full = file_minio_basepath + '/' + os.path.basename(file_thumb_full)  # minio的thumb文件全路径

    detail_data_list = []
    for tmp_file_name  in os.listdir(file_basepath):  #遍历文件夹，上传文件
        tmp_full = os.path.join(file_basepath, tmp_file_name)
        tmp_minio_full = file_minio_basepath+'/'+ tmp_file_name
        minio_client.upload_file(barrel, tmp_minio_full, tmp_full)  # 上传
        #注意这里得记录文件在Minio的地址
        detail_data_list.append({
            'file_path': tmp_file_name,
            'size': str(os.path.getsize(tmp_full))
        })

    # minio_client.upload_file(barrel, file_minio_browse2_full, file_jpg_full)    #上传
    # minio_client.upload_file(barrel, file_minio_thumb_full, file_thumb_full)    #上传
    # print(file_minio_browse2_full)
    # print(file_minio_thumb_full)


    ### 注意入库archive不管是不是新的，都可以直接入库，因为我们把archive的记录删了
    ## 入库archive
    archive_data = {
            'basic_product_id': cur_ProductID_int,
            'status': 1,
            'file_name': os.path.basename(file_minio_basepath),
            'file_path': file_minio_basepath,
            'is_deleted': 0,
            'reason': 'test03',
            'archive_time': datetime.now()
    }

    ## 注意，这里detail_data_list不用再手动注册了，我们前面遍历了文件夹，上传minio的时候，就记录了，可以直接用
    # detail_data_list = [
    #         {'file_path': os.path.basename(file_minio_browse2_full), 'size': str(os.path.getsize(file_jpg_full))},
    #         {'file_path': os.path.basename(file_minio_thumb_full), 'size': str(os.path.getsize(file_thumb_full))}
    # ]
    # 用archive打包器入库
    archive_manager.insert_archive_with_details(archive_data, detail_data_list)

    ## 入库product
    product_basic = {
        'basic_product_id': cur_ProductID_int,
        'is_valid': 1,
        'is_store': 1,  # 完整数据
        'source': 'ZY',  # 资源中心
        'is_archived': 1,
        'type': 'STANDARD',
        'platform': 'Satellite',
        'created_at': datetime.now(),
        'updated_at': datetime.now(),
        'is_deleted': 0
    }

    # 'POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))'
    str_polygon = 'POLYGON((' + str(data_meta['xml_top_left_longitude']) + ' ' + \
                  str(data_meta['xml_top_left_latitude']) + ',' + \
                  str(data_meta['xml_top_right_longitude']) + ' ' + \
                  str(data_meta['xml_top_right_latitude']) + ',' + \
                  str(data_meta['xml_bottom_right_longitude']) + ' ' + \
                  str(data_meta['xml_bottom_right_latitude']) + ',' + \
                  str(data_meta['xml_bottom_left_longitude']) + ' ' + \
                  str(data_meta['xml_bottom_left_latitude']) + ',' + \
                  str(data_meta['xml_top_left_longitude']) + ' ' + \
                  str(data_meta['xml_top_left_latitude']) + '))'  # 请注意，一个四边形，一定要5个点，最后一个点和第一个坐标相同

    product_detail_extra = {
        'thumb_url': file_minio_thumb_full,
        'preview_url': file_minio_browse2_full,
        'polygon': str_polygon,  # 'POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))'
        'xml_image_gsd': 30
    }
    product_detail_combine = data_meta | product_detail_extra
    # mysql入库
    inserted_id2 = db_inserter.insert_data('product_basic', product_basic)
    inserted_id3 = db_inserter.insert_data('product_detail', product_detail_combine)


    return












