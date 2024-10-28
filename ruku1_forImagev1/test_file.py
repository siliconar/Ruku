
from MinioClientWrapper import MinioClientWrapper

barrel = "hyspec"



# 示例使用
if __name__ == "__main__":
    # 初始化 Minio 客户端（单例）
    minio_client = MinioClientWrapper(
        endpoint="10.110.31.154:9000",
        access_key="minio_Q2xXTs",
        secret_key="minio_Jr48cB",
        secure=False    # 使用http
    )



    # # 下载文件
    # minio_client.download_file(barrel, "123/123.txt", "D:/workdir/2.txt")
    # # 上传文件
    # minio_client.upload_file(barrel, "123/2.txt", "D:/workdir/2.txt")

    # 上传文件夹
    minio_client.upload_folder(barrel, "D:/workdir/456","123/")

    