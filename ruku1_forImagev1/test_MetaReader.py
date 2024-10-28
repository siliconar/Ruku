from MetaReader import  MetaReader




# 使用示例
meta_file = 'C:/Users/SITP/Desktop/好看/ZY1E/2024-08-14/801488/meta.txt'  # meta.txt 文件的路径
translation_file = 'C:/Users/SITP/Desktop/sun04/rukuv1/translate_meta2label.csv'  # translation.csv 文件的路径

# 创建实例
translator = MetaReader(meta_file, translation_file)

# 获取翻译后的元数据
translated_metadata = translator.get_translated_metadata()

# 输出结果
print(translated_metadata)