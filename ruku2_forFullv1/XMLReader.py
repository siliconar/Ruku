import csv
from datetime import datetime
import xml.etree.ElementTree as ET


class XMLReader:
    def __init__(self, meta_file, translation_file):
        self.meta_file = meta_file
        self.translation_file = translation_file
        self.translation_dict = self.load_translation()

    def load_translation(self):
        translation = {}
        with open(self.translation_file, 'r', encoding='gb2312') as f:
            reader = csv.reader(f)
            next(reader)  # 跳过表头
            for row in reader:
                if len(row) == 3:  # 包含中文标签, 英文标签, 数据类型
                    chinese_label, english_label, data_type = row
                    translation[chinese_label.strip()] = (english_label.strip(), data_type.strip())
            return translation

    def load_meta(self):


        tree = ET.parse(self.meta_file)
        root = tree.getroot()

        metadata = {}
        for child in root:
            if child.text and child.text.strip():  # 过滤掉没有值的标签
                metadata[child.tag] = child.text.strip()
        return metadata


    def translate_meta(self):
        meta_data = self.load_meta()
        translated_meta = {}

        def convert_value(value, data_type):
            if data_type == "int":
                return int(value)
            elif data_type == "decimal":
                return float(value)
            elif data_type == "datetime":
                # 处理包含毫秒的时间字符串，截取到秒
                return datetime.strptime(value.split('.')[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                # return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            elif data_type == "string":
                return str(value)
            elif data_type == "varchar":
                return str(value)
            else:
                return value  # 默认不处理

        for key, value in meta_data.items():
            if key in self.translation_dict:  # 仅保留翻译文件中出现的标签
                translated_key, data_type = self.translation_dict[key]
                translated_meta[translated_key] = convert_value(value, data_type)
            # else:# 如果没有翻译，保留原始键

        return translated_meta

    def get_translated_metadata(self):
        return self.translate_meta()

