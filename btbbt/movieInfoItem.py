import scrapy

class movieInfo(scrapy.Item):
        spiderUrl = scrapy.Field()
        name = scrapy.Field()
        type = scrapy.Field()
        classInfo = scrapy.Field()
        createTime = scrapy.Field()
        editTime = scrapy.Field()
        allInfo = scrapy.Field()
        plugInfo = scrapy.Field()
        imgs = scrapy.Field()
        filestr = scrapy.Field()
        # bbs的板块ID
        bbsFid = scrapy.Field()
        bbsMainPostJson = scrapy.Field()
        bbsRelinesListJson = scrapy.Field()
        pass