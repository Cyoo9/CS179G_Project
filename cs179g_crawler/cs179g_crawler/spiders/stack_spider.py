import scrapy

from scrapy.selector import Selector
# from scrapy.linkextractors import LinkExtractor
# from scrapy.spiders import Rule

from ..items import QuestionItem 

class StackOverflowSpider(scrapy.Spider):
    name = "stack"
    allowed_domains = ["stackoverflow.com"]
    start_urls = []
    # rules = [
    #     Rule(LinkExtractor(allow=r'questions\?tab=newest&page=\b[0-9]\b'),         
    #     callback='parse', follow=True)
    # ]

    def __init__(self):
        url = 'http://stackoverflow.com/questions?tab=newest&page='

        for page in range(1, 3):
            self.start_urls.append(url + str(page))
             
    
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
