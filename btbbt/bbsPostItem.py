import scrapy


class bbsItem(scrapy.Item):
    bbsType = scrapy.Field() # 这里是区别，插入的内容是发新贴，还是回复
    fId = scrapy.Field() # 这里是板块ID。需要根据发帖的板块自行设置
    tId = scrapy.Field() # 帖子主表ID，主要用于回帖的时候，判断是谁的主贴
    subject = scrapy.Field() # 标题
    dataline = scrapy.Field() # 发帖时间戳
    attachment = scrapy.Field() # 是否有附件，附件个数
    # 下面是帖子内容表
    message = scrapy.Field()# 帖子详情
    # 附件主表
    fileName = scrapy.Field() # 附件名称
    attachmentUrl = scrapy.Field() # 附件地址
    pass

