# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class MovieprojectPipeline:
    def open_spider(self,spider):
        #取得爬虫对象中的mongodb客户端对象
        self.client=spider.client
        #创建或者打开集合
        self.infos_collection = self.client['moviedb']['infos']

    #处理item，把item中的 数据保存到mongodb中
    def process_item(self, item, spider):
        name=item['name']
        desc = item['desc']
        self.infos_collection.insert_one({"name":name,"desc":desc})
        return item
