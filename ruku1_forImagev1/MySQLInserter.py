

#Description
#MySQL注入器
#只提供3个功能，初始化；给某表注入一条数据，并返回主键ID；断开连接
#额外增加了3个功能


import pymysql


class MySQLInserter:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4'
            )
            self.cursor = self.connection.cursor()
            print("Connected to database successfully!")
        except pymysql.MySQLError as e:
            print(f"Error connecting to MySQL: {e}")

    # def insert_data(self, table, data):
    #     try:
    #         if not self.connection:
    #             self.connect()
    #
    #         # 获取列名和值
    #         columns = ', '.join(data.keys())
    #         values = ', '.join(['%s'] * len(data))
    #
    #         # 构建 SQL 语句
    #         sql = f"INSERT INTO {table} ({columns}) VALUES ({values})"
    #
    #         # 执行插入操作
    #         self.cursor.execute(sql, tuple(data.values()))
    #         self.connection.commit()
    #         print("Data inserted successfully!")
    #
    #         # 获取主键值
    #         last_insert_id = self.cursor.lastrowid
    #         print("Last inserted ID:", last_insert_id)
    #         return last_insert_id
    #
    #     except pymysql.MySQLError as e:
    #         print(f"Error inserting data: {e}")
    #         self.connection.rollback()
    #     # finally:
    #         # self.close()

    def insert_data(self, table, data):
        try:
            if not self.connection:
                self.connect()

            columns = ', '.join(data.keys())
            values = []
            placeholders = []

            for key, value in data.items():
                if isinstance(value, str) and value.startswith('POLYGON'):  # 检查是否是WKT格式
                    placeholders.append(f"ST_GeomFromText(%s)")
                else:
                    placeholders.append('%s')
                values.append(value)

            placeholders_str = ', '.join(placeholders)
            # 构建 SQL 语句
            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders_str})"

            # 执行插入操作
            self.cursor.execute(sql, tuple(values))
            self.connection.commit()

            # 获取主键值
            last_insert_id = self.cursor.lastrowid
            return last_insert_id
        except pymysql.MySQLError as e:
            print(f"Error inserting data: {e}")
            self.connection.rollback()
        # finally:
        #     self.close()




    def check_field_exists(self, table, field_name, field_value):
        """检查表中某字段（如fileID）是否存在某个值，返回布尔值"""
        try:
            if not self.connection:
                self.connect()

            sql = f"SELECT COUNT(*) FROM {table} WHERE {field_name} = %s"
            self.cursor.execute(sql, (field_value,))
            result = self.cursor.fetchone()
            return result[0] > 0
        except pymysql.MySQLError as e:
            print(f"Error checking field existence: {e}")
            return False
        # finally:
        #     self.close()

    def delete_by_field(self, table, field_name, field_value):
        """删除表中具有某字段（如fileID）等于某值的记录"""
        try:
            if not self.connection:
                self.connect()

            if self.check_field_exists(table, field_name, field_value):
                sql = f"DELETE FROM {table} WHERE {field_name} = %s"
                self.cursor.execute(sql, (field_value,))
                self.connection.commit()
                print(f"Record(s) with {field_name} = {field_value} deleted successfully.")
            else:
                print(f"No record found with {field_name} = {field_value}.")
        except pymysql.MySQLError as e:
            print(f"Error deleting record: {e}")
            self.connection.rollback()
        # finally:
        #     self.close()

    def update_by_field(self, table, field_name, field_value, new_data):
        """检查表中某字段（如fileID）是否唯一，若唯一则修改对应记录"""
        try:
            if not self.connection:
                self.connect()

            # 查询该字段值是否唯一
            sql = f"SELECT COUNT(*) FROM {table} WHERE {field_name} = %s"
            self.cursor.execute(sql, (field_value,))
            result = self.cursor.fetchone()

            if result[0] == 0:
                raise ValueError(f"No record found with {field_name} = {field_value}.")
            elif result[0] > 1:
                raise ValueError(f"Multiple records found with {field_name} = {field_value}. Only one expected.")
            else:
                # 构建更新语句
                update_columns = ', '.join([f"{key} = %s" for key in new_data.keys()])
                sql = f"UPDATE {table} SET {update_columns} WHERE {field_name} = %s"
                values = tuple(new_data.values()) + (field_value,)
                self.cursor.execute(sql, values)
                self.connection.commit()
                print(f"Record with {field_name} = {field_value} updated successfully.")
        except pymysql.MySQLError as e:
            print(f"Error updating record: {e}")
            self.connection.rollback()
        # finally:
        #     self.close()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection closed.")


