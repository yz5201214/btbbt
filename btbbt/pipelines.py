# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.files import FilesPipeline
# pymsql是pyton的数据库包
import pymysql.cursors,scrapy
import pandas as pd
# 要想使用redis模块，需要导入的不是redis ，而是redispy，需要py一下才行
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

    def process_item(self, movieInfo, spider):
        try:
            self.cursor.execute(
                """insert into F_M_INFO(F_ID, F_SPIDER_URL, F_NAME, F_TYPE, F_STATUS, F_ED2K_URL, F_DOWNLOAD_URL, F_CREATE_TIME, F_LAST_EDIT_TIME, F_ALL_INFO, F_MOVIE_IMGS, F_MOVIE_FILS)
                           value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",  # 纯属python操作mysql知识，不熟悉请恶补
                (movieInfo['id'],movieInfo['spiderUrl'],movieInfo['name'],movieInfo['type'],movieInfo['status'],movieInfo['ed2kUrl'],movieInfo['downLoadUrl'],movieInfo['createTime'],movieInfo['editTime'],movieInfo['allInfo'],movieInfo['imgs'],movieInfo['filestr'],))
            self.connect.commit()
        except Exception as e:
            e
        return movieInfo # 必须返回
    # 当spider关闭的时候，关闭数据库连接
    def close_spider(self,spider):
        self.connect.close()


class bbsMysqlPipline(object):
    def __init__(self):
        self.connect = pymysql.connect(
            host='d-flat.crosssee.cn',# 数据库地址
            port=3307,# 端口 注意是int类型
            db='ultrax',# 数据库名称
            user='root',# 用户名
            passwd='88888888',# 用户密码
            charset='utf8', # 字符编码集 ,注意这里，直接写utf8即可
            use_unicode=True)
        # 进行数据库连接初始化
        self.cursor = self.connect.cursor()

    def process_item(self, bbsItem, spider):
        try:
            fId = '2'# 这里是板块ID，通过统一配置来进行插入

            selectSQL = 'select max(pid)pid from pre_forum_post_tableid'
            df = pd.read_sql(selectSQL, self.connect)  # 读myql数据库
            onliyId = int(df['pid'].get_values()[0])+1

            # pid表
            self.cursor.execute(
                """insert into pre_forum_post_tableid(pid)
                           value (%s)""",
                # 纯属python操作mysql知识，不熟悉请恶补
                (onliyId,))
            self.connect.commit()

            self.cursor.execute(
                """insert into pre_forum_thread(tid,fid,posttableid,typeid,sortid,readperm,price,author,authorid,subject,dateline,lastpost,lastposter,views,replies,displayorder,highlight,digest,rate,special,attachment,moderated,closed,stickreply,recommends,recommend_add,recommend_sub,heats,status,isgroup,favtimes,sharetimes,stamp,icon,pushedaid,cover,replycredit,relatebytag,maxposition,bgcolor,comments,hidden)
                           value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",  # 纯属python操作mysql知识，不熟悉请恶补
                (onliyId,fId,'0','0','0','0','0','admin','1',bbsItem['subject'],bbsItem['dataline'],bbsItem['dataline'],
                'admin','1','0','0','0','0','0','0',
                bbsItem['attachment'],
                '0','0','0','0','0','0','0','32','0','0','0','-1','-1','0','0','0','0','1','','0','0',))
            self.connect.commit()

            # 帖子内容表
            self.cursor.execute(
                """insert into pre_forum_post(pid,fid,tid,first,author,authorid,subject,dateline,message,useip,port,invisible,anonymous,usesig,htmlon,bbcodeoff,smileyoff,parseurloff,attachment,rate,ratetimes,status,tags,comment,replycredit,position)
                           value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                # 纯属python操作mysql知识，不熟悉请恶补
                (onliyId,fId,onliyId,'1','admin','1',
                bbsItem['subject'],bbsItem['dataline'],bbsItem['message'],
                '127.0.0.1','8081','0','0','1','1','0','-1','0',
                bbsItem['attachment'],
                '0','0','0','','0','0','1',))
            self.connect.commit()

            if bbsItem['attachment'] is not None and bbsItem['attachment'] !='0':
                # 最后一位，用于附件详情表存放
                xStr = str(onliyId)
                xStr = xStr[len(xStr) - 1:len(xStr)]
                # 附件主表
                self.cursor.execute(
                    """insert into pre_forum_attachment(aid, tid, pid, uid, tableid, downloads)
                               value (%s, %s, %s, %s, %s, %s)""",
                    # 纯属python操作mysql知识，不熟悉请恶补
                    (onliyId,onliyId,onliyId,'1',xStr,'0',))
                self.connect.commit()
                # 附件内容表
                insert_sql = "INSERT INTO pre_forum_attachment_"+xStr+"(aid, tid, pid, uid, dateline,filename,filesize,attachment,remote,description,readperm,price,isimage,width,thumb,picid) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                self.cursor.execute(insert_sql,
                    # 纯属python操作mysql知识，不熟悉请恶补
                    # +xStr
                    (onliyId, onliyId, onliyId, onliyId, bbsItem['dataline'], bbsItem['fileName'],'1000',bbsItem['attachmentUrl'],'0','','0','0','0','0','0','0',))
                self.connect.commit()


            # 更新板块主题数目，今日发帖数等
            lastpost = ['test',bbsItem['subject'],bbsItem['dataline'],'admin']
            lastpostStr = '	'.join(lastpost)
            updateForumSql = "update pre_forum_forum set threads = threads +1 , posts = posts +1 , todayposts = todayposts +1 , lastpost = '"+lastpostStr+"' where fid = %s "
            self.cursor.execute(updateForumSql,(fId,))
            self.connect.commit()
        except Exception as e:
            e
            self.connect.rollback()
        return bbsItem # 必须返回

    # 当spider关闭的时候，关闭数据库连接
    def close_spider(self,spider):
        self.connect.close()