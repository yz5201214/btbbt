import scrapy


class bbsItem(scrapy.Item):
    subject = scrapy.Field() # 标题
    dataline = scrapy.Field() # 发帖时间戳
    attachment = scrapy.Field() # 是否有附件，附件个数
    # 下面是帖子内容表
    message = scrapy.Field()# 帖子详情
    # 附件主表
    fileName = scrapy.Field() # 附件名称
    attachmentUrl = scrapy.Field() # 附件地址
    pass

