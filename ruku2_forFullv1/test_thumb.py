from ImageResizer import  ImageResizer

if __name__ == "__main__":
    # 初始化ImageResizer
    resizer = ImageResizer()

    # 保存缩放后的图像
    resizer.save_resized_image('C:/Users/SITP/Desktop/801488/pic.png',300, 'C:/Users/SITP/Desktop/801488/pic_resized_image.jpg')

