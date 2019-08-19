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
import numpy
# scrapy 常用异常处理类
from scrapy.exceptions import DropItem
from scrapy.utils.project import get_project_settings

# 直接初始化redis连接，启动中间件的时候就可以启动redis了。毕竟是用于去重
# 如果当前链接被占用，那么会去链接db
redis_db = Redis(host='dev.kuituoshi.com',port=6379,password='88888888',db=4)
redis_data_btbbt = 'btbbt_cache'

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

class bbsMysqlPipline(object):
    def __init__(self):
        self.connect = pymysql.connect(
            host='www.findbt.com',  # 数据库地址
            port=3306,  # 端口 注意是int类型
            db='ultrax',# 数据库名称
            user='root',# 用户名
            passwd='cyfU2ypeM9AVeVzh',# 用户密码
            charset='utf8', # 字符编码集 ,注意这里，直接写utf8即可
            use_unicode=True)

        # 读取配置文件
        self.settings = get_project_settings()
        # 进行数据库连接初始化
        self.cursor = self.connect.cursor()
        # 初始化的时候，清空所有缓存，保证数据的时效性
        redis_db.flushdb()
        # 这里将插件中初始化的数据进行解析存储到redis，用于后续的spider爬取使用
        selectSQL = 'select * from pre_common_pluginvar'
        common = pd.read_sql(selectSQL, self.connect)# 影视大全参数配置表
        for commonItem in common.get_values():
            sItem = pd.Series(commonItem)
            # 影视分类
            if sItem[5] == 'fenlei':
                for xItem in sItem[7].split('\r\n'):
                    abc = xItem.split('=')
                    # 所有的分类全部进行redis初始化存储，用于后续的区分
                    redis_db.hset('fenlei', abc[0], abc[1])# 结构：key=1，value=电影
            # 地区分类
            if sItem[5] == 'diqu':
                for xItem in sItem[7].split('\r\n'):
                    abc = xItem.split('=')
                    # 所有的分类全部进行redis初始化存储，用于后续的区分
                    redis_db.hset('diqu', abc[1], abc[0])# 结构：key=大陆，value=1
            # 题材类型分类
            if sItem[5] == 'leixing':
                for xItem in sItem[7].split('\r\n'):
                    abc = xItem.split('=')
                    # 所有的分类全部进行redis初始化存储，用于后续的区分
                    redis_db.hset('leixing', abc[1], abc[0])# 结构：key=动作，value=1
            # 语言分类
            if sItem[5] == 'yuyan':
                for xItem in sItem[7].split('\r\n'):
                    abc = xItem.split('=')
                    # 所有的分类全部进行redis初始化存储，用于后续的区分
                    redis_db.hset('yuyan', abc[1], abc[0])# 结构：key=国语，value=1
        if redis_db.hlen(redis_data_btbbt) == 0:
            selectSQL = 'select * from gt_m_main_info'
            df = pd.read_sql(selectSQL, self.connect)  # 读myql数据库
            for url in df['F_SPIDER_URL'].get_values():
                # 数据库所有的值全部加入，用于去重处理
                # redis_data_btbbt 指定字典，key = url ,value = 0，这里要比对的实际是请求地址，那么拿url做key即可
                redis_db.hset(redis_data_btbbt, url, 0)
            # 增量爬取，还是初始化爬取
            # df['F_TYPE'].get_values() 是<class 'numpy.ndarray'>对象，这里指的琢磨下
            if str(df['F_TYPE'].get_values()).find('2')>=0:
                redis_db.hset(redis_data_btbbt, 'dramaSize', 'True')
            if str(df['F_TYPE'].get_values()).find('1')>=0:
                redis_db.hset(redis_data_btbbt, 'movieSize', 'True')

    def process_item(self, item, spider):
        if isinstance(item, movieInfo):
            if item['type'] =='2' and redis_db.hexists(redis_data_btbbt, item['spiderUrl']):
                # 这里是剧集更新操作
                self.dramaUpdate(item)
            else:
                # 主贴
                return_dict = self.bbsNewPosts(item)
                # 插件模块表写入
                bbsRealUrl = self.settings.get('MY_URL') + '/forum.php?mod=viewthread&tid='+str(return_dict['tid'])
                self.pulugDataIn(item,bbsRealUrl)
                if return_dict is not None:
                    self.gtMianAndReInfo(item,return_dict)
                    # 回帖集合
                    relinesList = json.loads(item['bbsRelinesListJson'])
                    if len(relinesList)>0:
                        self.bbsReplies(relinesList,item['spiderUrl'],item['bbsFid'],return_dict['tid'])
                else:
                    print('主贴插入异常警告')
        return item

    def dramaUpdate(self,movieItem):
        spiderUrl = movieItem['spiderUrl']
        newFileList = json.loads(movieItem['filestr'])
        dataLine = str(int(time.time()))
        if len(movieItem['name']) > 50:
            subjectStr = movieItem['name'][0:40] + "..."
        else:
            subjectStr = movieItem['name']
        # 只更新附件信息，如果没有，不做任何更新处理
        if len(newFileList)>0:
            try:
                # 这里是主贴的操作
                getGtMainInfoSql = 'select * from gt_m_main_info where F_SPIDER_URL = %s and F_TYPE = %s'
                self.cursor.execute(getGtMainInfoSql,(spiderUrl,movieItem['type'],))
                mainData = self.cursor.fetchone()
                F_B_TID = mainData[9]
                F_B_PID = mainData[10]
                F_B_AID = mainData[11]
                fileStr = mainData[8]
                oldFileList = json.loads(fileStr) # 数据库附件记录
                # incrementList = list(set(newFileList).difference(set(oldFileList)))# 新旧比对的附件增量
                # 当新旧附件个数不一样的时候，需要找出差集，上面这种list找差集，只能用在简单的list集合，对于list中是tupl的不行，因为list不能被哈希，tuple可以
                incrementList = []
                if len(newFileList) != len(oldFileList):
                    for newItem in newFileList:
                        # 在老附件里面找不到，作为增量附件
                        if fileStr.find(newItem['file_name']) <0:
                            incrementList.append(newItem)
                if len(incrementList)>0:
                    # 更新主贴的标题，最后修改时间，及附件个数
                    updateBBSThreadSql = "update pre_forum_thread set subject = '%s', lastpost = '%s', views = views +1 , attachment = attachment + '%d'" % (subjectStr,dataLine,len(incrementList),) +" where tid = %s and fid = %s"
                    print('主贴开始更新，更新的帖子，更新语句是:%s' % updateBBSThreadSql)
                    self.cursor.execute(updateBBSThreadSql,
                                        (F_B_TID,movieItem['bbsFid'],))
                    # 因为是主贴附件更新，所以需要更新主贴的附件详情，及主贴内容
                    attachList = []
                    for fileItem in incrementList:
                        xStr = F_B_TID[-1]
                        # 附件主表
                        self.cursor.execute(
                            """insert into pre_forum_attachment(aid, tid, pid, uid, tableid, downloads)
                                       value (%s, %s, %s, %s, %s, %s)""",
                            ('0', F_B_TID, F_B_PID, '1', xStr, '0',))
                        self.connect.commit()
                        fileAid = self.cursor.lastrowid
                        # 附件内容表
                        insert_sql = "INSERT INTO pre_forum_attachment_" + xStr + "(aid, tid, pid, uid, dateline,filename,filesize,attachment,remote,description,readperm,price,isimage,width,thumb,picid) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        self.cursor.execute(insert_sql,
                                            # 纯属python操作mysql知识，不熟悉请恶补
                                            # +xStr
                                            (fileAid, F_B_TID, F_B_PID, '1', dataLine, fileItem['file_name'],
                                             int(float(fileItem['file_size'][0:-1])),
                                             fileItem['file_url'], '0', '', '0', '0', '0', '0', '0', '0',))
                        attachList.append(str(fileAid))
                    attachList = attachList + json.loads(F_B_AID)
                    mainAllFileList = []
                    for mainA in attachList:
                        mainAllFileList.append('[attach]' + str(mainA) + '[/attach]<br>')
                    # 更新主贴内容
                    updateBBSPostSql = "update pre_forum_post set subject = '%s' ,dateline = '%s' ,message = concat(message,'%s') , attachment = attachment + '%d'" % (subjectStr,dataLine,''.join(mainAllFileList),len(incrementList),) +"where pid = %s and fid = %s and tid = %s"
                    self.cursor.execute(updateBBSPostSql,
                                        (F_B_PID, movieItem['bbsFid'],F_B_TID,))
                    # 更新自建主贴内容
                    updateGtMainSql = "update gt_m_main_info set f_edit_time = '%s' ,f_files = '%s' , f_b_aid = '%s'" % (movieItem['editTime'],movieItem['filestr'],json.dumps(attachList,ensure_ascii=False),) + "where f_spider_url = %s"
                    self.cursor.execute(updateGtMainSql,
                                        (spiderUrl,))
                    self.connect.commit()
            except Exception as e:
                print('剧集主贴更新异常对象 %s ，异常信息是 %s' % (movieItem,e,))
                self.connect.rollback()
                self.connect.commit()

        try:
            # 回帖更新操作
            relinesList = json.loads(movieItem['bbsRelinesListJson'])
            if len(relinesList) > 0:
                for relinesItem in relinesList:
                    getGtReInfoSql = 'select * from gt_m_replines_info where f_m_id = %s and f_r_id = %s'
                    self.cursor.execute(getGtReInfoSql, (spiderUrl,relinesItem['id'],))
                    relinesData = self.cursor.fetchone()
                    oldReInfo = relinesData[2] # 原回帖内容
                    reFileStr = relinesData[3]
                    oldReFileList = json.loads(reFileStr) #原回帖附件集合
                    newReFileList = json.loads(relinesItem['filestr']) # 新的附件集合
                    newReInfo = relinesItem['allInfo'] # 新的回帖内容
                    reIncrementList = []
                    if len(newReFileList) != len(oldReFileList):
                        for newItem in newReFileList:
                            # 在老附件里面找不到，作为增量附件
                            if reFileStr.find(newItem['file_name']) < 0:
                                reIncrementList.append(newItem)
                    F_B_TID = relinesData[4]
                    F_B_PID = relinesData[5]
                    F_B_AID = relinesData[6]
                    reAttachList = []
                    for reFileItem in reIncrementList:
                        xStr = F_B_TID[-1]
                        # 附件主表
                        self.cursor.execute(
                            """insert into pre_forum_attachment(aid, tid, pid, uid, tableid, downloads)
                                       value (%s, %s, %s, %s, %s, %s)""",
                            ('0', F_B_TID, F_B_PID, '1', xStr, '0',))
                        self.connect.commit()
                        fileAid = self.cursor.lastrowid
                        # 附件内容表
                        insert_sql = "INSERT INTO pre_forum_attachment_" + xStr + "(aid, tid, pid, uid, dateline,filename,filesize,attachment,remote,description,readperm,price,isimage,width,thumb,picid) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        self.cursor.execute(insert_sql,
                                            # 纯属python操作mysql知识，不熟悉请恶补
                                            # +xStr
                                            (fileAid, F_B_TID, F_B_PID, '1', dataLine,
                                             reFileItem['file_name'],
                                             int(float(reFileItem['file_size'][0:-1])),
                                             reFileItem['file_url'], '0', '', '0', '0', '0', '0', '0', '0',))
                        reAttachList.append(str(fileAid))
                        self.connect.commit()
                    # 新老附件合集
                    reAttachList = reAttachList + json.loads(F_B_AID)
                    reAllFileList = []
                    for reA in reAttachList:
                        reAllFileList.append('[attach]' + str(reA) + '[/attach]<br>')
                    if len(oldReInfo) != len(newReInfo) or len(reIncrementList)>0: # 理论上来说，字符数/附件个数的调整都算更新
                        print('回帖开始更新，主贴ID是: %s，更新的回帖是：%s' % (spiderUrl,relinesData,))
                        updateRePostSql = "update pre_forum_post set message = '%s' , attachment = attachment + '%d'" % (newReInfo+''.join(reAllFileList),len(reIncrementList),) + "where pid = %s and fid = %s and tid = %s"
                        self.cursor.execute(updateRePostSql,
                                            (F_B_PID, movieItem['bbsFid'], F_B_TID,))
                        # 更新自建表
                        updateGtReplinesSql = "update gt_m_replines_info set f_r_info = '%s' ,f_r_files = '%s' ,f_b_aid = '%s'" % (newReInfo,relinesItem['filestr'],json.dumps(reAttachList,ensure_ascii=False)) + "where f_r_id = %s and f_m_id = %s"
                        self.cursor.execute(updateGtReplinesSql,
                                            (relinesItem['id'],spiderUrl,))
                    self.connect.commit()
        except Exception as e:
            print('剧集回帖更新异常对象 %s ，异常信息是 %s' % (movieItem,e,))
            self.connect.rollback()
            self.connect.commit()

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
            print('自建表主表写入异常，异常对象 %s，错误信息 %s' % (movieItem,e,))

    # 这里是发布新贴
    def bbsNewPosts(self,movieItem):
        fId = movieItem['bbsFid']  # 这里是板块ID，通过统一配置来进行插入
        fileList = json.loads(movieItem['filestr'])# 附件集合
        dataLine = str(int(time.time()))
        try:
            if len(movieItem['name'])>50:
                subjectStr = movieItem['name'][0:30]+"..."
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
            print('新贴表插入异常对象 %s ,异常内容 %s' % (movieItem,e,))
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
                print('回贴表插入异常对象 %s ，异常信息：%s ' % (replinesItem,e,))
                self.connect.rollback()
                self.connect.commit()


    def pulugDataIn(self,movieItem, bbsRealUrl):
        dataLine = str(int(time.time()))
        # 分类
        if redis_db.hexists('fenlei', movieItem['type']):
            fenlei = movieItem['type']
        else:
            fenlei = '6' # 暂时6是其他
        nameList = movieItem['name'].split(',')
        classList = movieItem['classInfo'].split(',')
        # 类型
        if redis_db.hget('leixing', classList[2]):
            leixing = redis_db.hget('leixing', classList[2])
        else:
            leixing = '18' # 其他
        # 地区
        if redis_db.hget('diqu', classList[1]):
            diqu = redis_db.hget('diqu', classList[1])
        else:
            diqu = '11' # 其他
        # 语言 循环匹配语言，默认其他语言
        yuyan = []  # 其他
        for rKye in redis_db.hkeys('yuyan'):
            rValue = redis_db.hget('yuyan', rKye)
            if nameList[3].find(rKye.decode('utf-8')[0:1])>=0:
                yuyan.append(rValue.decode('utf-8'))
        if len(rValue)==0:
            yuyan.append('5') # 其他
        imglist = json.loads(movieItem['imgs'])
        if len(imglist) ==0:
            picUlr = '默认图片地址url'
        else:
            picUlr = imglist[0]
        # 年份
        nianfen = classList[0]
        if classList[0] == '更早':
            nianfen = '0'
        try:
            insertItemSql = 'insert into pre_plugin_xlwsq_ysdp_item(id, diynum, uid, author, title, pic, spic, fenlei, leixing, diqu, yuyan, nianfen, daoyan, yanyuan, dpname, jianjie, info, view, pf0, pf1, pf2, pf3, pf4, pf5, pf6, pf7, pfa, pfb, pfc, pfd, pfe, pff, pfg, pfh, dpcount, tuijian, banner, top, viewgroup, zhuanti, display, dateline)' \
                            'values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            # 这里是主贴内容
            self.cursor.execute(insertItemSql,('0','0','1','admin',nameList[1],picUlr,picUlr,fenlei,leixing,diqu,','.join(yuyan),nianfen,'导演','演员','总体评价 剧情 音乐 画面 特技',movieItem['name'],movieItem['plugInfo'],'0','5','5','5','5','5','0','0','0','0','0','0','0','0','0','0','0','0','0','','0','1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19','','0',dataLine,))
            sId = self.cursor.lastrowid
            # 附件sql
            downItemSql = 'insert into pre_plugin_xlwsq_ysdp_down(id, diynum, sid, downname, downurl, display, dateline) values(%s, %s, %s, %s, %s, %s, %s)'
            self.cursor.execute(downItemSql,('0', 1, sId, '下载链接', bbsRealUrl, '1', dataLine,))
            self.connect.commit()
        except Exception as e:
            print('插件表写入异常，异常对象%s , 异常内容： %s' % (movieItem,e))
            self.connect.rollback()
            self.connect.commit()

    # 当spider关闭的时候，关闭数据库连接
    def close_spider(self,spider):
        self.connect.close()