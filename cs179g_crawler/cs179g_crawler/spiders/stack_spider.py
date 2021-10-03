import scrapy

from scrapy import Spider
from scrapy.selector import Selector

from ..items import QuestionItem 

class StackOverflowSpider(scrapy.Spider):
    name = "stack"
    allowed_domains = ["stackoverflow.com"]
    start_urls = [
        "http://stackoverflow.com/questions?pagesize=50&sort=newest",
    ]
    
    def parse(self, response):
        questions = Selector(response).xpath('//div[@class="summary"]/h3')
        # time_created = Selector(response).xpath('//div[@class=')
        for question in questions:
            item = QuestionItem()
            item['title'] = question.xpath(
                'a[@class="question-hyperlink"]/text()').extract()[0]
            item['url'] = question.xpath(
                'a[@class="question-hyperlink"]/@href').extract()[0]
            item['date_posted'] = Selector(response).xpath(
                '//span[@class="relativetime"]/@title').extract()[0]
            
            yield item
