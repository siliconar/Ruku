


import  logging
from MySQLInserter import MySQLInserter      #用于操作数据库
from MinioClientWrapper import MinioClientWrapper  #minio操作类

class Check_Delete_1_Scene:
    def __init__(self,logger,db_inserter,minio_client,barrel):
        self.logger = logger
        self.db_inserter = db_inserter
        self.minio_client = minio_client
        self.barrel = barrel

    # 利用basic_product_id，检查数据库表是否完整
    # 检查项：
    # - product_basic表：条目是否存在
    # - product_detail表：条目是否存在
    # - product_archive表：条目是否存在;tar_url是否存在于Minio;tar_url是否与is_Store一致
    # - product_archive_detail表：文件是否按照规定齐全；缩略图入库，2个缩略图要齐全；tar入库，所有文件需要齐全；并且在minio中存在
    def Check_Exist_with_productID(self, basic_product_id):

        checklist = {
            'basic_product_id': -1,  # 键是字符串，值是变量 cur_ProductID_int
            'isExist_product_basic': -1,  # 在product_basic表是否存在
            'is_store':-1, # 是否是完整数据
            'isExist_product_archive': -1,   #在product_archive表是否存在
            'archive_id':-1,
            'browse_fullname':'-1',
            'thumb_fullname':'-1',
            'file_path':'-1'
        }
        tarPack_files = {
            'tar_fullname': '-1',
            'xml_fullname': '-1',
            'vntiff_fullname': '-1',
            'swtiff_fullname': '-1',
            'sw_RadCal_fullname': '-1',
            'vn_RadCal_fullname': '-1',
            'sw_Spectralresponse_fullname': '-1',
            'vn_Spectralresponse_fullname': '-1'
        }   # tar包独有的文件，需要单独检查

        # 记录basic_product_id
        checklist['basic_product_id'] = basic_product_id

        # 处理product_basic表
        vec_product_basic = self.db_inserter.fetch_by_field('product_basic', 'basic_product_id', checklist['basic_product_id'])
        if len(vec_product_basic) ==0:
            self.logger.error("product_basic表，basic_product_id="+str(basic_product_id)+"内容不存在")
            return False
        resdata_product_basic = vec_product_basic[0]  #以字典格式，取出这一行所有数据
        checklist['isExist_product_basic'] = 1   # 在product_basic表中存在
        checklist['is_store'] = resdata_product_basic['is_store']  # 是否是完整数据

        #处理product_detail表
        vec_product_detail = self.db_inserter.fetch_by_field('product_detail', 'basic_product_id',checklist['basic_product_id'])
        if len(vec_product_detail) !=1:
            self.logger.error("product_detail表，basic_product_id="+str(basic_product_id)+"内容不存在,或存在多条")
            return False

        #处理product_archive
        vec_product_archive = self.db_inserter.fetch_by_field('product_archive', 'basic_product_id',checklist['basic_product_id'])
        if len(vec_product_archive) !=1:
            self.logger.error("product_archive表，basic_product_id="+str(basic_product_id)+"内容不存在,或存在多条")
            return False
        resdata_product_archive = vec_product_archive[0]  #以字典格式，取出这一行所有数据
        checklist['isExist_product_archive'] = 1   # 在表中存在
        tarPack_files['tar_fullname'] = resdata_product_archive['tar_url']
        checklist['archive_id'] = resdata_product_archive['archive_id']
        filePath = resdata_product_archive['file_path']  # 文件路径
        checklist['file_path'] = resdata_product_archive['file_path']  # 文件路径


        #处理product_archive_detail
        vec_archive_detail = self.db_inserter.fetch_by_field('product_archive_detail', 'archive_id',
                                                              checklist['archive_id'])
        for i_detail in vec_archive_detail:  #遍历所有的文件记录,并归类记录
            filenamefull =  i_detail['file_path']
            if filenamefull.endswith('_thumb.jpg'):
                checklist['thumb_fullname'] = filePath +'/'+ filenamefull
            elif filenamefull.endswith('.jpg') or filenamefull.endswith('.png'):
                checklist['browse_fullname'] = filePath +'/'+ filenamefull
            elif filenamefull.endswith('.xml'):
                tarPack_files['xml_fullname'] = filePath +'/'+ filenamefull
            elif filenamefull.endswith('VN.geotiff'):
                tarPack_files['vntiff_fullname'] = filePath +'/'+ filenamefull
            elif filenamefull.endswith('SW.geotiff'):
                tarPack_files['swtiff_fullname'] = filePath +'/'+ filenamefull
            elif filenamefull.endswith('SWIR_RadCal.raw'):
                tarPack_files['sw_RadCal_fullname'] = filePath +'/'+ filenamefull
            elif filenamefull.endswith('VNIR_RadCal.raw'):
                tarPack_files['vn_RadCal_fullname'] = filePath +'/'+ filenamefull
            elif filenamefull.endswith('SWIR_Spectralresponse.raw'):
                tarPack_files['sw_Spectralresponse_fullname'] = filePath +'/'+ filenamefull
            elif filenamefull.endswith('VNIR_Spectralresponse.raw'):
                tarPack_files['vn_Spectralresponse_fullname'] = filePath +'/'+ filenamefull


        #正式开始检查
        # 检查checklist是否正常
        for i_key, i_value in checklist.items():
            if i_value == -1 or i_value == '-1':
                self.logger.error("checklist检查：" + str(i_key) + "为-1")
                return  False

        # 检查tar包是否正常
        if checklist['is_store'] == 1:
            for i_key, i_value in tarPack_files.items():
                if i_value == -1 or i_value == '-1':
                    self.logger.error("tar包检查：" + str(i_key) + "为-1")
                    return False

        # minio检查
        # 检查两个缩略图是否minio正常
        bexist = self.minio_client.file_exists(self.barrel, checklist['browse_fullname'])
        if not bexist:
            self.logger.error("minio检查： browse_fullname不存在")
            return False
        bexist = self.minio_client.file_exists(self.barrel, checklist['thumb_fullname'])
        if not bexist:
            self.logger.error("minio检查： thumb_fullname不存在")
            return False

        # 检查tar包所有文件是否minio正常
        if checklist['is_store'] == 1:
            for i_key, i_name in tarPack_files.items():
                bexist = self.minio_client.file_exists(self.barrel, i_name)
                if not bexist:
                    self.logger.error("minio检查： " + i_key +"|"+ i_name +"不存在")
                    return False

        # 走到这里说明检查全部通过
        self.checklist = checklist
        self.tarPack_files = tarPack_files



        return True


    # 利用basic_product_id，删除数据库和minio中，所有相关内容
    def realDelete_All_with_productID(self,basic_product_id):

        bcheck = self.Check_Exist_with_productID(basic_product_id)
        if not bcheck:
            self.logger.error("删除前的检查失败")
            return False

        # --- 开始删除

        # - 删除product_basic表
        self.db_inserter.delete_by_field('product_basic', 'basic_product_id', basic_product_id)
        # - 删除product_detail表
        self.db_inserter.delete_by_field('product_detail', 'basic_product_id', basic_product_id)
        # - 删除product_archive表
        self.db_inserter.delete_by_field('product_archive', 'basic_product_id', basic_product_id)

        # - 获取product_archive_detail表所有文件
        allfiles_list = self.db_inserter.fetch_by_field('product_archive_detail', 'archive_id', self.checklist['archive_id'])

        # - 删除product_archive_detail表
        self.db_inserter.delete_by_field('product_archive_detail', 'archive_id', self.checklist['archive_id'])

        # - minio文件删除
        filepath = self.checklist['file_path']
        for i_detail in allfiles_list:  #遍历所有的文件记录
            filename =  i_detail['file_path']
            filefullname = filepath +'/'+ filename

            # 把丫在minio上删除了
            self.minio_client.delete_file(self.barrel, filefullname)
            self.logger.info("删除内容: "+filefullname)

        return True