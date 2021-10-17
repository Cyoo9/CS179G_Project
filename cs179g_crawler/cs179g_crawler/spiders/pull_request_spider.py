import scrapy  

from scrapy.selector import Selector 
from ..items import PullRequestsItem 

class PullRequestSpider(scrapy.Spider):
    name = 'pullrequests'
    allowed_domains = ["github.com"]
    start_urls = []

    def __init__(self):
        url = 'https://github.com/facebook/react/pull/'

        for page in range(0, 0): #crawled 1 -> 22544 (range is from 1 to 22545) pages, currently deactivate to prevent accidental duplication in json. 
            self.start_urls.append(url + str(page))

    def parse(self, response):
        item = PullRequestsItem()
        item['title'] = response.xpath('/html/body/div[4]/div/main/div[2]/div/div/div[2]/div[1]/div[1]/div/h1/span[1]/text()').extract()
        item['id'] = response.xpath('/html/body/div[4]/div/main/div[2]/div/div/div[2]/div[1]/div[1]/div/h1/span[2]/text()').extract()
        item['linked_issue'] = response.xpath('/html/body/div[4]/div/main/div[2]/div/div/div[2]/div[3]/div/div[2]/div/div[6]/form/div[2]/a/text()').extract()
        yield item     
        