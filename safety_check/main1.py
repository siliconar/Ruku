
# 该程序用于
# - 检查基于product_basic中，所有的basic_product_id是否正常


from fun_check_delete_1_scene import *
from datetime import datetime, timedelta
from MySQL_BatchController import *

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
# - 检查基于product_basic中，所有的basic_product_id是否正常

# 制作一个检查器
obj_checker = Check_Delete_1_Scene(logger,db_inserter,minio_client,barrel)



# 制作一个批处理器
db_batchcontroller = MySQL_BatchController(
    host='10.110.31.155',
    user='root',
    password='sitp@nais@22611',
    database='hyspec_dev'
)

total_cnt = db_batchcontroller.get_primary_key_count('product_basic','basic_product_id')
logger.info("共：" + str(total_cnt) + "条记录")

# 获取yield的生成器
batch_size = 100;   #批处理数量
cc_generator = db_batchcontroller.fetch_primary_keys_in_batches('product_basic', 'basic_product_id', batch_size, reset=False)

logger.info("开始检查")
cnt =0
while cnt<total_cnt:
    res_1 = next(cc_generator)   #获取的basic_product_id会以数组形式存于res_1
    cnt = cnt + len(res_1)

    for i_id in res_1:
        bcheck = obj_checker.Check_Exist_with_productID(7321)
        if not bcheck:
            logger.error("第" + str(i_id) + "条记录出现错误")

    logger.info("检查进度" + str(int(cnt/total_cnt*100)) + "%")

db_batchcontroller.close()




#####  最后清理环境

db_inserter.close()









