from Fun_main_withXML import *






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


#####  程序正式开始
filexml = 'D:/workdir/杂乱tar包/ZY1E_AHSI_E89.27_N38.38_20191028_000668_L1A0000007321/ZY1E_AHSI_E89.27_N38.38_20191028_000668_L1A0000007321.xml'  # xml 文件的路径
fun_withXML(filexml, db_inserter, obj_thumb_resizer,minio_client,barrel,archive_manager)



#####  最后清理环境

db_inserter.close()









