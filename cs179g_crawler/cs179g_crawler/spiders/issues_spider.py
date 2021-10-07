import scrapy  

from scrapy.selector import Selector 
from ..items import IssuesItem 

class IssuesSpider(scrapy.Spider):
    name = 'issues'
    allowed_domains = ["github.com"]
    start_urls = []

    def __init__(self):
        url = 'https://github.com/facebook/react/issues?q=is%3Aissue+is%3Aclosed&page='

        for page in range(1, 408): 
            self.start_urls.append(url + str(page))

    def parse(self, response):
        issues = Selector(response).xpath('//div[starts-with(@id, "issue")]/div')
        for issue in issues:
            item = IssuesItem()
            item['title'] = issue.xpath('a[@class="d-block d-md-none position-absolute top-0 bottom-0 left-0 right-0"]/@aria-label').extract()[0]
            item['url'] = issue.xpath('a[@class="d-block d-md-none position-absolute top-0 bottom-0 left-0 right-0"]/@href').extract()[0]

            yield item     
