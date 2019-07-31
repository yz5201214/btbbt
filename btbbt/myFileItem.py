import scrapy

class MyFileItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    file_urls = scrapy.Field()
    file_name = scrapy.Field()
    files = scrapy.Field()