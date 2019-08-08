import scrapy

class movieInfo(scrapy.Item):
        id = scrapy.Field()
        spiderUrl = scrapy.Field()
        name = scrapy.Field()
        type = scrapy.Field()
        status = scrapy.Field()
        ed2kUrl = scrapy.Field()
        downLoadUrl = scrapy.Field()
        createTime = scrapy.Field()
        editTime = scrapy.Field()
        allInfo = scrapy.Field()
        imgs = scrapy.Field()
        filestr = scrapy.Field()
        pass