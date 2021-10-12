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
        issues = Selector(response).xpath('//div[starts-with(@id, "issue")]/div/div[@class="flex-auto min-width-0 p-2 pr-3 pr-md-2"]')
        for issue in issues:
            item = IssuesItem()
            item['title'] = issue.xpath('a[@class="Link--primary v-align-middle no-underline h4 js-navigation-open markdown-title"]/text()').extract()[0]
            item['url'] = issue.xpath('a[@class="Link--primary v-align-middle no-underline h4 js-navigation-open markdown-title"]/@href').extract()[0]
            item['date'] = issue.xpath('div[@class="d-flex mt-1 text-small color-text-secondary"]/span/relative-time/@datetime').extract()[0]
            item['status'] = issue.xpath('span[@class="labels lh-default d-block d-md-inline"]/a/@data-name').extract()
            yield item     
