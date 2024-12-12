

from fun_check_delete_1_scene import *
from datetime import datetime, timedelta


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

# ---------- 自动设置日志
LogPath = 'C:/Users/SITP/Desktop/sun04/'
LogName = LogPath + "Log_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"

# 创建日志记录器
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 创建格式化器
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 文件Handler
file_handler = logging.FileHandler(LogName)  # 替换为你的日志文件名
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# 控制台Handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ---------- 开始

#测试检查
obj_checker = Check_Delete_1_Scene(logger,db_inserter,minio_client,barrel)

bcheck = obj_checker.Check_Exist_with_productID(7321)

if bcheck:
    logger.info("检查成功")
else:
    logger.info("检查失败")

#测试删除
# obj_checker = Check_Delete_1_Scene(logger,db_inserter,minio_client,barrel)
# bcheck = obj_checker.realDelete_All_with_productID(7321)


#####  最后清理环境

db_inserter.close()





