from XMLReader import  XMLReader




# 使用示例
meta_file = 'D:/workdir/杂乱tar包/ZY1E_AHSI_E89.27_N38.38_20191028_000668_L1A0000007321/ZY1E_AHSI_E89.27_N38.38_20191028_000668_L1A0000007321.xml'  # xml 文件的路径
translation_file = 'C:/Users/SITP/Desktop/sun04/ruku2v1/translate_xml2label.csv'  # translation.csv 文件的路径

# 创建实例
translator = XMLReader(meta_file, translation_file)

# 获取翻译后的元数据
translated_metadata = translator.get_translated_metadata()

# 输出结果
print(translated_metadata)