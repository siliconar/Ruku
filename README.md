# Ruku1

两个文件夹，分别包含两个python工程
- ruku1_forImagev1用于入库爬虫缩略图
- ruku2_forFullv1用于入库完整数据

## ruku1_forImagev1
1. 程序可以根据整个文件夹【逐层遍历搜索】，全部入库
1. 目前需要输入文件夹组织方式为:  路径/文件夹名/【卫星编号】/【2024-04-10】/【景号】
    需要输入的为  路径/文件夹名
1. 入口为main.py

### 冲突处理逻辑
1. basic_product_id存在
- 跳过，打印已经存在，不做任何处理。

## ruku2_forFullv1
1. 程序只能单景入库
1. 目前需要将所有入库文件放入一个文件夹中，并指定xml地址
1.入口为main_withXML.py

### 冲突处理逻辑
1. basic_product_id存在
- 清理原来的archive表和archive_detail
- 删除minio文件
- 清理原来的product的2个表

