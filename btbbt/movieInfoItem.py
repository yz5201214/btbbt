import scrapy

class movieInfo(scrapy.Item):
        id = scrapy.Field()
        name = scrapy.Field()
        type = scrapy.Field()
        status = scrapy.Field()
        downLoadUrl = scrapy.Field()
        createTime = scrapy.Field()
        editTime = scrapy.Field()
        pass