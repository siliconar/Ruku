from minio import Minio
from minio.error import S3Error
import os


class MinioClientWrapper:
    # _instance = None  # 用于存放单例实例
    #
    # def __new__(cls, *args, **kwargs):
    #     if cls._instance is None:
    #         cls._instance = super().__new__(cls)
    #     return cls._instance
    #
    # def __init__(self, endpoint, access_key, secret_key, secure=False):
    #     """
    #     初始化Minio客户端（确保只执行一次初始化）
    #     :param endpoint: MinIO服务器的地址（如"play.min.io"）
    #     :param access_key: MinIO的访问密钥
    #     :param secret_key: MinIO的密钥
    #     :param secure: 是否使用HTTPS（默认不使用）
    #     """
    #     if not hasattr(self, 'client'):  # 确保初始化只执行一次
    #         self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


    def __init__(self, endpoint, access_key, secret_key, secure=False):
        """
        初始化Minio客户端
        :param endpoint: MinIO服务器的地址（如"play.min.io"）
        :param access_key: MinIO的访问密钥
        :param secret_key: MinIO的密钥
        :param secure: 是否使用HTTPS（默认不使用）
        """
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

    def upload_file(self, bucket_name, object_name, file_path):
        """
        上传文件到指定的bucket和路径
        :param bucket_name: MinIO的bucket名称
        :param object_name: MinIO中的对象路径（例如 'folder/file.txt'）
        :param file_path: 本地文件路径
        """
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            print(f"File {file_path} uploaded to {bucket_name}/{object_name} successfully.")
        except S3Error as err:
            print(f"File upload failed: {err}")

    def download_file(self, bucket_name, object_name, file_path):
        """
        从指定的bucket和路径下载文件到本地
        :param bucket_name: MinIO的bucket名称
        :param object_name: MinIO中的对象路径
        :param file_path: 下载到的本地文件路径
        """
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            print(f"File {bucket_name}/{object_name} downloaded to {file_path} successfully.")
        except S3Error as err:
            print(f"File download failed: {err}")

    def upload_folder(self, bucket_name, folder_path, minio_base_path):
        """
        上传整个文件夹到指定的bucket和路径
        :param bucket_name: MinIO的bucket名称
        :param folder_path: 本地文件夹路径
        :param minio_base_path: MinIO中的根路径（如 'my-folder/'）
        """
        try:
            # 遍历文件夹中的所有文件
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, folder_path)
                    minio_object_name = os.path.join(minio_base_path, relative_path).replace("\\", "/")

                    # 上传每个文件到 MinIO
                    self.upload_file(bucket_name, minio_object_name, local_file_path)

            print(f"Folder {folder_path} uploaded to {bucket_name}/{minio_base_path} successfully.")
        except S3Error as err:
            print(f"Folder upload failed: {err}")