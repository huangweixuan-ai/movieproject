import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from pymongo.mongo_client import MongoClient
from movieproject.items import MovieprojectItem


class MovieSpider(CrawlSpider):
    #初始化爬虫对象调用该方法
    def __init__(self):
        #调用父类的方法
        super().__init__(self)
        #访问mongodb数据库
        self.client=MongoClient("localhost",27017)
        #创建或者打开urls集合
        self.url_connection = self.client['moviedb']['urls']
    #销毁爬虫对象，回调该方法
    def __del__(self):
        self.client.close()

    name = 'mv'
    #allowed_domains = ['www.xxx.com']
    start_urls = ['http://www.4567kan.com/frim/index1.html']
    link = LinkExtractor(allow=r'/frim/index1-\d+\.html')
    rules = (
        Rule(link, callback='parse_item', follow=False),
    )
    #解析每一个页码对应的页面，并且获取电影的详情
    def parse_item(self, response):
        li_list = response.xpath('/html/body/div[1]/div/div/div/div[2]/ul/li')
        for li in li_list:
            detial_url = "http://www.4567kan.com"+ li.xpath('./div/a/@href').extract_first()
            # print(detial_url)

            #查询mongodb数据库中的url集合中有没有包含详情的url
            cursor = self.url_connection.find({"url":detial_url})
            if cursor.count()==0:
                #当前的url没有访问过
                print("该url没有被访问，可以进行数据的爬取...")
                #保存当前的url到urls集合中
                self.url_connection.insert_one({"url":detial_url})
                #发起一个新的请求，提取电影详情页面的信息
                yield scrapy.Request(url=detial_url,callback=self.parse_detail)
            else:
                #当前的url已经访问过了
                print("当前url已经访问过，无需再访问")
                pass
            # yield scrapy.Request(url=detial_url, callback=self.parse_detail)

    #解析电影详情页面的， 解析出电影的名称和描述信息
    def parse_detail(self, response):
        #获取电影名称
        name = response.xpath('/html/body/div[1]/div/div/div/div[2]/h1/text()').extract_first()

        #获取电影简介,电影描述信息
        desc = response.xpath('/html/body/div[1]/div/div/div/div[2]/p[5]/span[2]//text()').extract_first()
        desc = ''.join(desc)
        print(f"电影名称:{name}\n电影简介:{desc}")

        item = MovieprojectItem()
        item['name']=name
        item['desc']=desc
        yield item