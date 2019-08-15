# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.files import FilesPipeline
from btbbt.movieInfoItem import movieInfo
# pymsql是pyton的数据库包
import pymysql.cursors,scrapy,json,time
import pandas as pd
# 要想使用redis模块，需要导入的不是redis ，而是redispy，需要py一下才行
from redis import Redis
# scrapy 常用异常处理类
from scrapy.exceptions import DropItem

# 直接初始化redis连接，启动中间件的时候就可以启动redis了。毕竟是用于去重
# 如果当前链接被占用，那么会去链接db
redis_db = Redis(host='192.168.31.143',port=6379,password='',db=4)
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
    def get_media_requests(self,item , info):
        # 只有文件下载的时候才需要
        if 'file_urls' in item.keys():
            for fileUrl in item['file_urls']:
                yield scrapy.Request(fileUrl, meta={'item': item})
        return item

class mysqlPipline(object):
    def __init__(self):
        self.connect = pymysql.connect(
            host='192.168.31.143',# 数据库地址
            port=3306,# 端口 注意是int类型
            db='spider_movies',# 数据库名称
            user='root',# 用户名
            passwd='123456',# 用户密码
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

class bbsMysqlPipline(object):
    def __init__(self):
        self.connect = pymysql.connect(
            host='192.168.31.143',  # 数据库地址
            port=3306,  # 端口 注意是int类型
            db='ultrax',# 数据库名称
            user='root',# 用户名
            passwd='123456',# 用户密码
            charset='utf8', # 字符编码集 ,注意这里，直接写utf8即可
            use_unicode=True)
        # 进行数据库连接初始化
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        if isinstance(item, movieInfo):
            # 主贴
            return_dict = self.bbsNewPosts(item)
            if return_dict is not None:
                self.gtMianAndReInfo(item,return_dict)
                # 回帖集合
                relinesList = json.loads(item['bbsRelinesListJson'])
                if len(relinesList)>0:
                    self.bbsReplies(relinesList,item['spiderUrl'],item['bbsFid'],return_dict['tid'])
            else:
                print('主贴插入异常警告')
        return item

    def gtMianAndReInfo(self,movieItem,return_dict):
        try:
            gtMainInsetSql = 'insert into gt_m_main_info(F_SPIDER_URL, F_NAME, F_TYPE, F_CLASS_INFO, F_CREATE_TIME, F_EDIT_TIME, F_INFO, F_IMGS, F_FILES, F_B_TID, F_B_PID, F_B_AID) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.cursor.execute(gtMainInsetSql,
                                (movieItem['spiderUrl'], movieItem['name'], movieItem['type'], movieItem['classInfo'],
                                 movieItem['createTime'], movieItem['editTime'], movieItem['allInfo'], movieItem['imgs'], movieItem['filestr'],
                                 return_dict['tid'], return_dict['pid'], return_dict['aid'].replace('[attach]','').replace('[/attach]<br>',''),))
            self.connect.commit()
        except Exception as e:
            self.connect.rollback();
            self.connect.commit()
            print('自建表信息---写入异常，错误信息 %s' % e)

    # 这里是发布新贴
    def bbsNewPosts(self,movieItem):
        fId = movieItem['bbsFid']  # 这里是板块ID，通过统一配置来进行插入
        fileList = json.loads(movieItem['filestr'])# 附件集合
        dataLine = str(int(time.time()))
        try:
            if len(movieItem['name'])>50:
                subjectStr = movieItem['name'][0:40]+"..."
            else:
                subjectStr = movieItem['name']
            # pid表
            self.cursor.execute(
                """insert into pre_forum_post_tableid(pid)
                           value (%s)""",
                ('0',))
            self.connect.commit()
            onliyId = self.cursor.lastrowid
            # 帖子主表
            self.cursor.execute(
                """insert into pre_forum_thread(tid,fid,posttableid,typeid,sortid,readperm,price,author,authorid,subject,dateline,lastpost,lastposter,views,replies,displayorder,highlight,digest,rate,special,attachment,moderated,closed,stickreply,recommends,recommend_add,recommend_sub,heats,status,isgroup,favtimes,sharetimes,stamp,icon,pushedaid,cover,replycredit,relatebytag,maxposition,bgcolor,comments,hidden)
                           value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (onliyId, fId, '0', '0', '0', '0', '0', 'admin', '1', subjectStr, dataLine,
                 dataLine,
                 'admin', '1', '0', '0', '0', '0', '0', '0',
                 str(len(fileList)),
                 '0', '0', '0', '0', '0', '0', '0', '32', '0', '0', '0', '-1', '-1', '0', '0', '0', '0', '1', '', '0',
                 '0',))
            self.connect.commit()
            attachList = []
            if len(fileList) >0:
                # 最后一位，用于附件详情表存放
                xStr = str(onliyId)[-1]
                for fileItem in fileList:
                    # 附件主表
                    self.cursor.execute(
                        """insert into pre_forum_attachment(aid, tid, pid, uid, tableid, downloads)
                                   value (%s, %s, %s, %s, %s, %s)""",
                        ('0', onliyId, onliyId, '1', xStr, '0',))
                    self.connect.commit()
                    fileAid = self.cursor.lastrowid
                    # 附件内容表
                    insert_sql = "INSERT INTO pre_forum_attachment_" + xStr + "(aid, tid, pid, uid, dateline,filename,filesize,attachment,remote,description,readperm,price,isimage,width,thumb,picid) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    self.cursor.execute(insert_sql,
                                        # 纯属python操作mysql知识，不熟悉请恶补
                                        # +xStr
                                        (fileAid, onliyId, onliyId, onliyId, dataLine, fileItem['file_name'], int(float(fileItem['file_size'][0:-1])),
                                         fileItem['file_url'], '0', '', '0', '0', '0', '0', '0', '0',))
                    attachList.append('[attach]' + str(fileAid) + '[/attach]<br>')
                # 帖子内容表
            self.cursor.execute(
                """insert into pre_forum_post(pid,fid,tid,first,author,authorid,subject,dateline,message,useip,port,invisible,anonymous,usesig,htmlon,bbcodeoff,smileyoff,parseurloff,attachment,rate,ratetimes,status,tags,comment,replycredit,position)
                           value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (onliyId, fId, onliyId, '1', 'admin', '1',
                 subjectStr, dataLine, movieItem['allInfo']+''.join(attachList),
                 '127.0.0.1', '8081', '0', '0', '1', '1', '0', '-1', '0',
                 str(len(fileList)),
                 '0', '0', '0', '', '0', '0', '1',))
            # 更新板块主题数目，今日发帖数等
            lastpost = ['亏坨屎', subjectStr, dataLine, 'admin']
            lastpostStr = '	'.join(lastpost)
            updateForumSql = "update pre_forum_forum set threads = threads +1 , posts = posts +1 , todayposts = todayposts +1 , lastpost = '" + lastpostStr + "' where fid = %s "
            self.cursor.execute(updateForumSql, (fId,))
            # 事物处理，一次性提交
            self.connect.commit()
            # 返回，如果有需要回帖，则需要
            return_dict = {
                'tid': onliyId,
                'pid': onliyId,
                'aid': json.dumps(attachList, ensure_ascii=False)
            }
            return return_dict
        except Exception as e:
            print('新贴表插入异常对象 %s' % movieItem)
            print('新贴异常 %s' % e)
            self.connect.rollback()
            self.connect.commit()
        return None

    def bbsReplies(self,replinesList,mId,fId,tId):
        for replinesItem in replinesList:
            fileList = json.loads(replinesItem['filestr'])  # 附件集合
            dataLine = str(int(time.time()))
            try:
                # pid表
                self.cursor.execute(
                    """insert into pre_forum_post_tableid(pid)
                               value (%s)""",
                    ('0',))
                self.connect.commit()
                onliyId = self.cursor.lastrowid
                # 附件处理
                attachList = []
                # 最后一位，用于附件详情表存放，回帖表必须要根据主贴的TID位数来进行附件分表
                xStr = str(tId)[-1]
                for fileItem in fileList:
                    # 附件主表
                    self.cursor.execute(
                        """insert into pre_forum_attachment(aid, tid, pid, uid, tableid, downloads)
                                   value (%s, %s, %s, %s, %s, %s)""",
                        ('0', tId, onliyId, '1', xStr, '0',))
                    self.connect.commit()
                    fileAid = self.cursor.lastrowid
                    # 附件内容表
                    insert_sql = "INSERT INTO pre_forum_attachment_" + xStr + "(aid, tid, pid, uid, dateline,filename,filesize,attachment,remote,description,readperm,price,isimage,width,thumb,picid) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    self.cursor.execute(insert_sql,
                                        (fileAid, tId, onliyId, '1', dataLine, fileItem['file_name'], int(float(fileItem['file_size'][0:-1])),
                                         fileItem['file_url'], '0', '', '0', '0', '0', '0', '0', '0',))
                    attachList.append('[attach]'+str(fileAid)+'[/attach]<br>')
                # 回复内容表
                bbsRepliseMsg = replinesItem['allInfo'] + ''.join(attachList)
                self.cursor.execute(
                    """insert into pre_forum_post(pid,fid,tid,first,author,authorid,subject,dateline,message,useip,port,invisible,anonymous,usesig,htmlon,bbcodeoff,smileyoff,parseurloff,attachment,rate,ratetimes,status,tags,comment,replycredit,position)
                               value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (onliyId, fId, tId, '0', 'admin', '1',
                     '--', dataLine, bbsRepliseMsg,
                     '127.0.0.1', '8081', '0', '0', '1', '1', '-1', '-1', '0',
                     str(len(fileList)),
                     '0', '0', '1024', '0', '0', '0', '0',))

                # 回复自建表
                gtReInsertSql = 'insert into gt_m_replines_info(F_R_ID, F_M_ID, F_R_INFO, F_R_FILES, F_B_TID, F_B_PID, F_B_AID) values (%s,%s,%s,%s,%s,%s,%s) '
                self.cursor.execute(gtReInsertSql,
                                    (replinesItem['id'], mId, replinesItem['allInfo'], replinesItem['filestr'],
                                     tId,onliyId,json.dumps(attachList,ensure_ascii=False).replace('[attach]','').replace('[/attach]<br>',''),))
                self.connect.commit()
            except Exception as e:
                print('回贴表插入异常对象 %s ' % replinesItem)
                print('回贴表插入异常，错误信息 %s' % e)
                self.connect.rollback()
                self.connect.commit()

    # 当spider关闭的时候，关闭数据库连接
    def close_spider(self,spider):
        self.connect.close()