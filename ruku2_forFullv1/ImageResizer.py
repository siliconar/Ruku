from PIL import Image


class ImageResizer:
    def __init__(self, ):
        """
        初始化ImageResizer对象
        :param image_path: 图像文件的路径
        """



    def save_resized_image(self, image_path, target_width, save_path):
        """
        保存调整大小后的图像
        :param target_width: 目标宽度，单位像素
        :param save_path: 缩放后图像的保存路径
        """

        image = Image.open(image_path)
        original_width, original_height = image.size
        # 计算按比例缩放后的高度
        target_height = int((target_width / original_width) * original_height)
        # 缩放图像
        resized_image = image.resize((target_width, target_height), Image.Resampling.LANCZOS)

        resized_image.save(save_path)
        print(f"图像已成功保存至 {save_path}")
