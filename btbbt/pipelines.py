# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.files import FilesPipeline
# pymsql是pyton的数据库包
import pymysql.cursors,scrapy
import pandas as pd
# 要想使用redis模块，需要导入的不是redis ，而是redis-py，需要py一下才行
from redis import Redis
# scrapy 常用异常处理类
from scrapy.exceptions import DropItem


# 直接初始化redis连接，启动中间件的时候就可以启动redis了。毕竟是用于去重
# 如果当前链接被占用，那么会去链接db
redis_db = Redis(host='d-flat.gangway.cn',port=6379,password='88888888',db=4)
redis_data_dict = 'f_url'

class BtbbtPipeline(object):
    def process_item(self, item, spider):
        return item

class btFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        # 获取文件下载路径
        item = request.meta['item']
        #  = urlparse(request.url).path
        # 根据文件名称保存
        # return join(basename(dirname(path)),basename(path))
        # return '%s' % (basename(item['file_name']))
        return '%s' % item['file_name']
    # 只能通过这个方法进行item传递
    def get_media_requests(self, item, info):
        # 只有文件下载的时候才需要
        if 'file_urls' in item.keys():
            for fileUrl in item['file_urls']:
                yield scrapy.Request(fileUrl, meta={'item': item})

class mysqlPipline(object):
    def __init__(self):
        self.connect = pymysql.connect(
            host='d-flat.crosssee.cn',# 数据库地址
            port=3307,# 端口 注意是int类型
            db='spidertest',# 数据库名称
            user='root',# 用户名
            passwd='88888888',# 用户密码
            charset='utf8', # 字符编码集 ,注意这里，直接写utf8即可
            use_unicode=True)
        # 进行数据库连接初始化
        self.cursor = self.connect.cursor()
        # 没错清理全部的key，重新进行匹配，保证数据的时效性
        redis_db.flushdb()
        # 这里是过期时间，过期时间单位：秒
        # redis_db.expire(redis_data_dict,20)
        if redis_db.hlen(redis_data_dict) == 0:
            selectSQL = 'select F_SPIDER_URL from F_M_INFO'
            df = pd.read_sql(selectSQL,self.connect)# 读myql数据库
            for url in df['F_SPIDER_URL'].get_values():
                # 数据库所有的值全部加入，用于去重处理
                # redis_data_dict 指定字典，key = url ,value = 0，这里要比对的实际是请求地址，那么拿url做key即可
                redis_db.hset(redis_data_dict,url,0)
                redis_db.hset(redis_data_dict,'movieSize',len(df))



    def process_item(self, item, spider):
        try:
            self.cursor.execute(
                """insert into F_M_INFO(F_ID, F_SPIDER_URL, F_NAME, F_TYPE, F_STATUS, F_ED2K_URL, F_DOWNLOAD_URL, F_CREATE_TIME, F_LAST_EDIT_TIME, F_ALL_INFO)
                           value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",  # 纯属python操作mysql知识，不熟悉请恶补
                (item['id'],
                 item['spiderUrl'],
                 item['name'],  #item里面定义的字段和表字段对应
                 item['type'],
                 item['status'],
                 item['ed2kUrl'],
                 item['downLoadUrl'],
                 item['createTime'],
                 item['editTime'],
                 item['allInfo'],))
            self.connect.commit()
        except Exception as e:
            e
        return item # 必须返回
    # 当spider关闭的时候，关闭数据库连接
    def close_spider(self,spider):
        self.connect.close()
