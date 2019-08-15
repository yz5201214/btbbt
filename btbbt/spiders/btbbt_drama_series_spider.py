# 剧集爬取
import scrapy,time,json
from btbbt.myFileItem import MyFileItem
from btbbt.movieInfoItem import movieInfo

# 这了一定要注意Spider 的首字母大写
class btbbtDramaSeriesSpider(scrapy.Spider):
    name = 'drama'
    bbsTid = '36'
    '''
    custom_settings = {
        'ITEM_PIPELINES':{'btbbt.pipelines.btFilesPipeline': 1}
    }
    '''
    start_urls = [
        'http://btbtt.org/forum-index-fid-950.htm',# 剧集首页
    ]

    def parse(self, response):
        next_ur = None
        num = None
        '''
        start_request 已经爬取到了网页内容，parse是将内容进行解析，分析，获取本身我自己需要的数据内容
        流程是：1。爬取指定的内容页 2.通过返回内容自定义规则提取数据
        :param response: 页面返回内容
        :return: 必须返回
        ::attr("href")
        '''
        # 开始解析其中具体电影内容
        movidTableList = response.css('#threadlist table')
        for table in movidTableList:
            icoClass = table.css('span::attr("class")').extract_first()
            # 滤除公告板块，考虑到图片的多样性，凡是不是公告。全部爬取
            if icoClass.find('icon-top') <0:
                # 获取电影帖子url
                allMovieUrlList = table.css('a.subject_link')
                for movieUrl in allMovieUrlList:
                    realUrl = response.urljoin(movieUrl.css('a::attr("href")').extract_first())

                    '''
                    # 利用redis去重，在redis_data_dict中是否已经存在该URL，如果存在不爬取
                    if redis_db.hexists(redis_data_dict, realUrl):
                        # 如果存在，直接剔除该item，但是这里有个问题，如果我是线程执行，那么redis的生存周期怎么设置
                        self.log('该电影已经入库，无需重复入库 %s' % realUrl)
                        break
                    '''
                    yield scrapy.Request(realUrl,callback=self.dramaParse)
        '''
        已经测试可
        # 下面是翻页请求next_ur
        next_pages = response.css('div.page a')
        self.log(next_pages[len(next_pages)-1].css('a::text').extract_first())
        if next_pages[len(next_pages)-1].css('a::text').extract_first() == '▶':
            next_ur = response.urljoin(next_pages[len(next_pages)-1].css('a::attr("href")').extract_first())
        # 下面开始翻页请求 
        self.log("下一页地址：%s" % next_ur)
        # 第一次爬取，爬到所有翻页没有停止
        if next_ur is not None and num is None:
            yield scrapy.Request(next_ur,callback=self.parse)
        # 往后的增量爬取，只取前十页数据即可
        if next_ur is not None and num is not None and num >=10:
            num = num + 1
            redis_db.set('pageNum', num)
            yield scrapy.Request(next_ur,callback=self.parse)
        '''
    def dramaParse(self,response):
        onlyId = response.url.split('/')[-1]
        movieTtpeStr = "".join(response.css('div.bg1.border.post h2 a::text').extract()).replace('\t', '').replace('\r','').replace('\n', '')
        movieNameStr = "".join(response.css('div.bg1.border.post h2::text').extract()).replace('\t', '').replace('\r','').replace('\n', '')
        movieTtpeList = movieTtpeStr.replace('][', ',').replace('[', '').replace(']', '').split(',')
        movieNameList = movieNameStr.replace('][', ',').replace('[', '').replace(']', '').split(',')
        # 文件存放路径 spider名称/年份/最后详细地址
        cusPath = [self.name,movieTtpeList[0],response.url.split('/')[-1]]
        movieImgs = []
        # 详细信息中的图片文件下载，按照原路径保存
        if len(response.css('p img')) > 0:
            for imgList in response.css('p img'):
                myfileItem = MyFileItem()
                if imgList.css('img::attr("src")').extract_first().find('http') == -1:
                    myfileItem['file_urls'] = [response.urljoin(imgList.css('img::attr("src")').extract_first())]
                    myfileItem['file_name'] = imgList.css('img::attr("src")').extract_first()
                    movieImgs.append(myfileItem['file_name'])
                    yield myfileItem

        mainPostAttach = response.css('#body table:nth-child(2) div.attachlist')
        allAttachLen = len(response.css('div.attachlist'))
        movieFiles = []
        if mainPostAttach is not None and len(mainPostAttach) ==1:
            allAttachLen = allAttachLen -1
            x = 0
            for tableTrItem in mainPostAttach.css('table tr'):
                if tableTrItem.css('a') is not None and len(tableTrItem.css('a')) > 0:
                    url = tableTrItem.css('a::attr("href")').extract_first()
                    btName = tableTrItem.css('a::text').extract_first()
                    btSize = tableTrItem.css('td')[2].css('td::text').extract_first()  # 这里获取大小
                    # 种子文件下载地址
                    movieFileUrl = response.urljoin(url)
                    myfileItem = MyFileItem()
                    if btName.find('.torrent') >= 0:
                        # 目前只下载种子
                        realFileName = onlyId + '_' + str(x) +'.torrent'
                        # 下载地址
                        myfileItem['file_urls'] = [movieFileUrl.replace('dialog', 'download')]
                        # 存储位置 ,文件名称不能含有中文，所以存储的时候采用
                        myfileItem['file_name'] = '/'.join(cusPath)+'/'+realFileName
                        # 自己存库用的附件列表
                        fileDict = {
                            'file_name':btName,
                            'file_url':myfileItem['file_name'],
                            'file_size':btSize
                        }
                        movieFiles.append(fileDict)
                        x = x + 1
                        yield myfileItem
        movieText = response.css('#body table')[1].css('p').extract()

        # 剧集信息入库处理
        if movieTtpeStr is not None:
            movieItem = movieInfo()
            movieItem['spiderUrl'] = response.url
            movieItem['type'] = '2'# 2剧集
            # ,隔开的数组[年份,地区,类型,广告类型]
            movieItem['classInfo'] = movieTtpeStr.replace('][', ',').replace('[', '').replace(']', '')
            # ,隔开的数组[下载类型,名称,文件类型/大小,字幕类型,分辨率]
            movieItem['name'] = movieNameStr.replace('][', ',').replace('[', '').replace(']', '')
            movieItem['createTime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            movieItem['editTime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            movieItem['allInfo'] = "".join(movieText).replace('<img src="/upload/','<img src="http://192.168.31.143:8081/upload/data/attachment/forum/upload/')
            movieItem['imgs'] = json.dumps(movieImgs,ensure_ascii=False)
            movieItem['filestr'] = json.dumps(movieFiles,ensure_ascii=False)
            movieItem['bbsFid'] = self.bbsTid
            bbsReplinesList = []
            if allAttachLen > 0:
                # 这里是全部的回帖内容
                messageTableList = response.css('#body table')
                # 从第三个开始，前面都是垃圾
                for x in range(3, len(messageTableList)):
                    # 无字片源暂时过滤
                    repliesInfo = ''.join(messageTableList[x].css('p').extract()).replace('%7C', '|')
                    attach = messageTableList[x].css('div.attachlist')
                    # 有附件的回帖处理，有些更新是网盘更新，下面处理
                    if repliesInfo.find('无字片源') < 0 and len(attach) == 1:
                        movieFiles = []
                        # 这里获取该回复楼层的DIV_ID，用于下次更新的时候匹配楼层，是否更新
                        msgDivId = messageTableList[x].css('div.message::attr("id")').extract_first()
                        x = 0
                        for tableTrItem in attach.css('table tr'):
                            if tableTrItem.css('a') is not None and len(tableTrItem.css('a')) > 0:
                                url = tableTrItem.css('a::attr("href")').extract_first()
                                # 显示用的名字
                                btName = tableTrItem.css('a::text').extract_first()
                                btSize = tableTrItem.css('td')[2].css('td::text').extract_first()  # 这里获取大小
                                # 种子文件下载地址 ,我只下载种子
                                if btName.find('.torrent') >= 0:
                                    movieFileUrl = response.urljoin(url)
                                    myfileItem = MyFileItem()
                                    myfileItem['file_urls'] = [movieFileUrl.replace('dialog', 'download')]
                                    # 最后是存储用的名字
                                    myfileItem['file_name'] = '/'.join(cusPath) + '/' + msgDivId + '/' + str(
                                        x) + '.torrent'
                                    fileDict = {
                                        'file_name': btName,
                                        'file_url': myfileItem['file_name'],
                                        'file_size': btSize
                                    }
                                    movieFiles.append(fileDict)
                                    yield myfileItem
                        # 回帖内容
                        fRepliesItem = {
                            'id':msgDivId,
                            'allInfo':repliesInfo,
                            'filestr':json.dumps(movieFiles, ensure_ascii=False)
                        }
                        bbsReplinesList.append(fRepliesItem)
                    # 无附件内容，百度网盘模式更新
                    if repliesInfo.find('无字片源') < 0 and len(attach) == 0 and repliesInfo.find('pan.baidu.com') > 0:
                        # 这里获取该回复楼层的DIV_ID，用于下次更新的时候匹配楼层，是否更新
                        msgDivId = messageTableList[x].css('div.message::attr("id")').extract_first()
                        fRepliesItem = {
                            'id': msgDivId,
                            'allInfo': repliesInfo,
                            'filestr':json.dumps(movieFiles, ensure_ascii=False)
                        }
                        bbsReplinesList.append(fRepliesItem)
            movieItem['bbsRelinesListJson'] = json.dumps(bbsReplinesList,ensure_ascii=False)
            yield movieItem

