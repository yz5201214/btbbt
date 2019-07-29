# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.files import FilesPipeline
# pymsql是pyton的数据库包
import pymysql.cursors
from urllib.parse import urlparse
from os.path import basename,dirname,join


class BtbbtPipeline(object):
    def process_item(self, item, spider):
        return item

class btFilesPipeline(FilesPipeline):

    def file_path(self, request, response=None, info=None):
        # 获取文件下载路径
        path = urlparse(request.url).path

        # 根据文件名称保存
        # return join(basename(dirname(path)),basename(path))
        return '%s/%s.torrent' % (basename(dirname(path)), basename(path))

class mysqlPipline(object):
    def __init__(self):
        self.connect = pymysql.connect(
            host='d-flat.crosssee.cn',# 数据库地址
            port='3307',# 端口
            db='spidertest',# 数据库名称
            user='root',# 用户名
            passwd='88888888',# 用户密码
            charset='utf-8', # 字符编码集
            use_unicode=True)
        # 进行数据库连接初始化
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        self.cursor.execute(
            """insert into F_M_INFO(F_ID, F_NAME, F_TYPE, F_STATUS, F_DOWNLOAD_URL, F_CREATE_TIME, F_LAST_EDIT_TIME)
                       value (%s, %s, %s, %s, %s, %s, %s)""",  # 纯属python操作mysql知识，不熟悉请恶补
            (item['id'],
             item['name'],  # item里面定义的字段和表字段对应
             item['type'],
             item['status'],
             item['downLoadUrl'],
             item['createTime'],
             item['editTime']))
        self.connect.commit()
        return item # 必须返回
