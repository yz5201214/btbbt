import scrapy,re,time,uuid,json
from btbbt.myFileItem import MyFileItem
from btbbt.movieInfoItem import movieInfo
from btbbt.bbsPostItem import bbsItem
from btbbt.pipelines import redis_db, redis_data_dict

class btbbt(scrapy.Spider):# 需要继承scrapy.Spider类
    # 电影爬取
    name = 'btbbt' # 定义spider名称

    start_urls = [
        # http://btbtt.org/forum-index-fid-951.htm
        'http://btbtt.org/forum-index-fid-951.htm',
    ]
    '''
    # 另外一种初始化连接的写法
    # 由此方法通过下面的连接进行页面爬取，下面是全部需要爬取的网页地址
    # 如果需要带参数的爬取，那么只能用下面的方法初始化连接
    def start_requests(self):
        tag = getattr(self, 'tag', None) # 获取tag参数值
        start_urls = [
            'http://btbtt.org/index-index-page-2.htm',
        ]
        for url in start_urls:
            # 注意里面的参数写法，爬取到网页的内容交给parse进行处理
            yield scrapy.Request(url=url,callback=self.parse)
    '''
    # 针对网页爬取完成后的内容进行处理
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
        if redis_db.hget(redis_data_dict,'movieSize') is not None:
            # 初始化第0页开始
            if redis_db.get('pageNum') is None:
                num = 0
            else:
                num = int(redis_db.get('pageNum'))

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
                    # 利用redis去重，在redis_data_dict中是否已经存在该URL，如果存在不爬取
                    if redis_db.hexists(redis_data_dict, realUrl):
                        # 如果存在，直接剔除该item，但是这里有个问题，如果我是线程执行，那么redis的生存周期怎么设置
                        self.log('该电影已经入库，无需重复入库 %s' % realUrl)
                        break
                    yield scrapy.Request(realUrl,callback=self.movieParse)
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

    # 获取电影详细信息，磁力链接地址，种子下载地址
    def movieParse(self,response):
        onlyId = uuid.uuid4().hex
        movieTtpeStr = "".join(response.css('div.bg1.border.post h2 a::text').extract()).replace('\t','').replace('\r','').replace('\n','')
        movieNameStr = "".join(response.css('div.bg1.border.post h2::text').extract()).replace('\t','').replace('\r','').replace('\n','')
        movieMagnet = ''
        movieEd2k = ''
        baiduWp = ''
        movieFileUrl = ''
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

        # 文件路径处理
        cusPath = [self.name]
        movieTtpeList = movieTtpeStr.replace('][', ',').replace('[', '').replace(']', '').split(',')
        movieNameList = movieNameStr.replace('][', ',').replace('[', '').replace(']', '').split(',')
        for x in range(0, 4):
            cusPath.append(movieTtpeList[x])
        cusPath.append(movieNameList[1].replace('/','*'))

        # 附件列表
        movieFiles = []
        fileList = response.css('div.attachlist table tr')
        for item in fileList:
            if item.css('a') is not None and len(item.css('a'))>0:
                url = item.css('a::attr("href")').extract_first()
                btName = item.css('a::text').extract_first()
                btSize = item.css('td.grey::text').extract_fist() # 这里获取大小
                myfileItem = MyFileItem()
                # 种子文件下载地址
                movieFileUrl = response.urljoin(url)
                myfileItem = MyFileItem()
                myfileItem['file_urls'] = [movieFileUrl.replace('dialog','download')]
                myfileItem['file_name'] = '/'.join(cusPath)+'/'+btName
                movieFiles.append(btName)
                yield myfileItem

        movieText = response.css('p').extract()
        # 移除最后一个P元素
        movieText.pop()
        if len(movieText)>0:
            movieStr = "".join(movieText).replace('\t','').replace('\r','').replace('\n','')
            p = re.compile(r'magnet:\?xt=urn:btih:[0-9a-fA-F]{40}')
            m = p.findall(movieStr)
            if len(m) >0:
                movieMagnet = m[0]
            p = re.compile(r'ed2k://\|file\|.*?\|/')
            m = p.findall(movieStr)
            if len(m) >0:
                movieEd2k = m[0]

        # 电影信息入库处理
        if movieTtpeStr is not None:
            movieItem = movieInfo()
            movieItem['id'] = onlyId
            movieItem['spiderUrl'] = response.url
            # ,隔开的数组[年份,地区,类型,广告类型]
            movieItem['type'] = movieTtpeStr.replace('][', ',').replace('[', '').replace(']', '')
            # ,隔开的数组[下载类型,名称,文件类型/大小,字幕类型,分辨率]
            movieItem['name'] = movieNameStr.replace('][', ',').replace('[', '').replace(']', '')
            movieItem['status'] = 1
            if movieMagnet is not None:
                movieItem['downLoadUrl'] = movieMagnet
            if movieEd2k is not None:
                movieItem['ed2kUrl'] = movieEd2k
            movieItem['createTime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            movieItem['editTime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            movieItem['allInfo'] = "".join(movieText).replace('<img src="/upload/','<img src="http://localhost:8081/upload/data/attachment/forum/upload/')
            if len(movieImgs)>0:
                movieItem['imgs'] = json.dumps(movieImgs)
            if len(movieFiles)>0:
                movieItem['filestr'] = json.dumps(movieFiles)
            yield movieItem

            # 然后开始bbs入库
            bbs = bbsItem()
            bbs['subject'] = movieItem['type'] + movieItem['name']
            bbs['dataline'] = str(int(time.time()))
            bbs['attachment'] = str(len(movieFiles))
            bbs['message'] = movieItem['allInfo']
            if len(movieFiles)>0:
                bbs['fileName'] = movieFiles[0]
                bbs['attachmentUrl'] = movieFiles[0]
            yield bbs




