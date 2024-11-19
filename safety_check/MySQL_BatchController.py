import pymysql


#Description
#MySQL批量操作器
# 主要功能
# 1. 分批获取主键
# 这个的意思是，我一次获取100个主键，那么就反馈回100个。如果再次获取100个，那么会继续往下获取100个，不会从头开始。除非

class MySQL_BatchController:

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

    # 新增方法：分批获取主键
    def fetch_primary_keys_in_batches(self, table, primary_key_column, batch_size, reset=False):
        """
        分批获取主键，每次返回固定数量的主键值，支持连续调用。

        :param table: 表名
        :param primary_key_column: 主键列名
        :param batch_size: 每次获取的主键数量
        :param reset: 是否重置 last_id，从头开始查询
        :return: 一个生成器，逐批返回主键值的列表
        """
        try:
            if not self.connection:
                self.connect()

            # 如果需要重置，则从 0 开始
            if reset or not hasattr(self, '_last_id'):
                self._last_id = 0

            while True:
                # 使用 WHERE 条件和 LIMIT 实现分页查询
                query = f"""
                SELECT {primary_key_column}
                FROM {table}
                WHERE {primary_key_column} > %s
                ORDER BY {primary_key_column} ASC
                LIMIT %s
                """
                self.cursor.execute(query, (self._last_id, batch_size))
                rows = self.cursor.fetchall()

                # 如果没有更多数据，退出循环
                if not rows:
                    break

                # 提取主键值列表
                primary_keys = [row[0] for row in rows]
                yield primary_keys

                # 更新最后的主键值
                self._last_id = primary_keys[-1]

        except pymysql.MySQLError as e:
            print(f"Error fetching primary keys: {e}")


    def get_primary_key_count(self, table, primary_key_column):
        """
        获取表中主键总数。

        :param table: 表名
        :param primary_key_column: 主键列名
        :return: 主键总数（整数）
        """
        try:
            if not self.connection:
                self.connect()

            # 构建查询语句
            query = f"SELECT COUNT({primary_key_column}) FROM {table}"
            self.cursor.execute(query)
            result = self.cursor.fetchone()

            # 返回总数
            return result[0]
        except pymysql.MySQLError as e:
            print(f"Error fetching primary key count: {e}")
            return 0


    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection closed.")
