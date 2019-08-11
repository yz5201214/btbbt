# 剧集爬取
import scrapy,uuid,time
from btbbt.myFileItem import MyFileItem

# 这了一定要注意Spider 的首字母大写
class btbbtDramaSeriesSpider(scrapy.Spider):
    name = 'drama'
    custom_settings = {
        'ITEM_PIPELINES':{'btbbt.pipelines.btFilesPipeline': 1}
    }
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
        onlyId = uuid.uuid4().hex
        movieTtpeStr = "".join(response.css('div.bg1.border.post h2 a::text').extract()).replace('\t', '').replace('\r','').replace('\n', '')
        movieNameStr = "".join(response.css('div.bg1.border.post h2::text').extract()).replace('\t', '').replace('\r','').replace('\n', '')

        # 文件路径处理
        cusPath = [self.name]
        movieTtpeList = movieTtpeStr.replace('][', ',').replace('[', '').replace(']', '').split(',')
        movieNameList = movieNameStr.replace('][', ',').replace('[', '').replace(']', '').split(',')
        for x in range(0,4):
            cusPath.append(movieTtpeList[x])
        cusPath.append(movieNameList[1].replace('/','*'))


        # 详细信息中的图片文件下载，按照原路径保存
        if len(response.css('p img')) > 0:
            for imgList in response.css('p img'):
                myfileItem = MyFileItem()
                if imgList.css('img::attr("src")').extract_first().find('http') == -1:
                    myfileItem['file_urls'] = [response.urljoin(imgList.css('img::attr("src")').extract_first())]
                    myfileItem['file_name'] = imgList.css('img::attr("src")').extract_first()
                    yield myfileItem

        '''
            这里是主贴的附件，因为剧集里面有主贴附件，还会有回帖附件
            只所以要分开，主要也是因为论坛主贴的附件无法分类处理，只能也按照回帖处理同剧集，当时不同清晰度，不同翻译组的附件
        '''
        mainPostAttach = response.css('#body table:nth-child(2) div.attachlist')
        allAttachLen = len(response.css('div.attachlist'))
        if mainPostAttach is not None and len(mainPostAttach) ==1:
            allAttachLen = allAttachLen -1
            for tableTrItem in mainPostAttach.css('table tr'):
                if tableTrItem.css('a') is not None and len(tableTrItem.css('a')) > 0:
                    url = tableTrItem.css('a::attr("href")').extract_first()
                    btName = tableTrItem.css('a::text').extract_first()
                    # btSize = tableTrItem.css('td.grey::text').extract_fist()  # 这里获取大小
                    myfileItem = MyFileItem()
                    # 种子文件下载地址
                    movieFileUrl = response.urljoin(url)
                    myfileItem = MyFileItem()
                    myfileItem['file_urls'] = [movieFileUrl.replace('dialog', 'download')]
                    myfileItem['file_name'] = '/'.join(cusPath)+'/'+btName
                    yield myfileItem
        '''
            剔除主贴的附件列表，如果附件div还有的情况下，从第三个table开始盘回帖内容，看谁有，下载谁的，主要是为了区别同剧集，不同字幕组，分辨率的情况
        '''
        if allAttachLen > 0:
            messageTableList = response.css('#body table')
            # 从第三个开始，前面都是垃圾
            for x in range(3,len(messageTableList)):
                attach = messageTableList[x].css('div.attachlist')
                if attach is not None and len(attach) ==1:
                    for tableTrItem in attach.css('table tr'):
                        if tableTrItem.css('a') is not None and len(tableTrItem.css('a')) > 0:
                            # 这部分的附件内容标题说明
                            mssageTitle = messageTableList[x].css('p strong::text').extract_first()
                            if mssageTitle is None:
                                mssageTitle = str(x)+'楼无名种子'
                            url = tableTrItem.css('a::attr("href")').extract_first()
                            btName = tableTrItem.css('a::text').extract_first()
                            # btSize = tableTrItem.css('td.grey::text').extract_fist()  # 这里获取大小
                            myfileItem = MyFileItem()
                            # 种子文件下载地址 ,我只下载种子
                            if btName.find('.torrent')>=0:
                                movieFileUrl = response.urljoin(url)
                                myfileItem = MyFileItem()
                                myfileItem['file_urls'] = [movieFileUrl.replace('dialog', 'download')]
                                myfileItem['file_name'] = '/'.join(cusPath) + '/'+mssageTitle+'/' + btName
                                yield myfileItem

