import scrapy,re,time,random
from btbbt.myFileItem import MyFileItem
from btbbt.movieInfoItem import movieInfo

class btbbt(scrapy.Spider):# 需要继承scrapy.Spider类
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
            # 滤除公告板块，实际
            if icoClass.find('icon-post') >-1:
                # 获取电影帖子url
                allMovieUrlList = table.css('a.subject_link')
                for movieUrl in allMovieUrlList:
                    realUrl = response.urljoin(movieUrl.css('a::attr("href")').extract_first())
                    yield scrapy.Request(realUrl,callback=self.movieParse)

        '''
        # 下面是翻页请求
        next_pages = response.css('div.page a')
        self.log(next_pages[len(next_pages)-1].css('a::text').extract_first())
        if next_pages[len(next_pages)-1].css('a::text').extract_first() == '▶':
            next_ur = mainUrl + next_pages[len(next_pages)-1].css('a::attr("href")').extract_first()
        # 下面开始翻页请求
        self.log("下一页地址：%s" % next_ur)
        if next_ur is not None:
            next_ur = next_ur
            yield scrapy.Request(next_ur,callback=self.parse)
        '''

    # 获取电影详细信息，磁力链接地址，种子下载地址
    def movieParse(self,response):
        movieTtpe = "".join(response.css('div.bg1.border.post h2 a::text').extract()).replace('\t','').replace('\r','').replace('\n','')
        movieName = "".join(response.css('div.bg1.border.post h2::text').extract()).replace('\t','').replace('\r','').replace('\n','')
        self.log(movieTtpe+'--------'+movieName)
        movieMagnet = ''
        movieEd2k = ''
        baiduWp = ''
        movieFileUrl = ''
        movieText = response.css('p').extract()
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
        if movieTtpe is not None:
            movieItem = movieInfo()
            movieItem['id'] = str(random.randint(0,10000))
            movieItem['type'] = movieTtpe.replace('][',',').replace('[','').replace(']','')
            movieItem['name'] = movieName.replace('][',',').replace('[','').replace(']','')
            movieItem['status'] = 1
            if movieMagnet is not None:
                movieItem['downLoadUrl'] = movieMagnet
            if movieEd2k is not None:
                movieItem['ed2kUrl'] = movieEd2k
            movieItem['createTime'] = time.time()
            movieItem['editTime'] = time.time()
            yield movieItem
        '''
        # 附件列表
        fileList = response.css('div.attachlist a')
        for item in fileList:
            url = item.css('a::attr("href")').extract_first()
            # 种子文件下载地址
            movieFileUrl = response.urljoin(url)
            yield scrapy.Request(movieFileUrl,callback=self.btSeedParse)
        self.log("电影信息:{0}，磁力连接地址：{1}，种子文件下载地址：{2}".format(movieName,movieMagnet,movieFileUrl))
        '''


    def btSeedParse(self,response):
        btFileUrl = response.urljoin(response.css('div.width.border.bg1 a::attr("href")').extract_first())
        self.log("下载地址：%s" % btFileUrl)
        myfileItem = MyFileItem()
        myfileItem['file_urls'] = [btFileUrl]
        return myfileItem



