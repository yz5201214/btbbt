# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.files import FilesPipeline
from urllib.parse import urlparse
from os.path import basename,dirname,join


class BtbbtPipeline(object):
    def process_item(self, item, spider):
        return item

class btFilesPipeline(FilesPipeline):

    def file_path(self, request, response=None, info=None):
        # 获取文件下载路径
        path = urlparse(request.url).path

        # 根据文件名称保存
        # return join(basename(dirname(path)),basename(path))
        return '%s/%s.torrent' % (basename(dirname(path)), basename(path))


