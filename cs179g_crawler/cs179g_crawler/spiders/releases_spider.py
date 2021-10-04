import scrapy  

from scrapy.selector import Selector 
from ..items import ReleasesItem

class ReleasesSpider(scrapy.Spider):
    name = 'releases'
    allowed_domains = ["github.com"]
    start_urls = ['https://github.com/vuestorefront/storefront-ui/releases']

    def parse(self, response):
        releases = Selector(response).xpath('//div[@class="col-12 col-md-9 col-lg-10 px-md-3 py-md-4 release-main-section commit open float-left"]')
        for release in releases:
            items = ReleasesItem()
            items['tag'] = release.xpath('div[@class="release-header"]/div/div/a/text()').extract()[0]
            items['url'] = release.xpath('div[@class="release-header"]/div/div/a/@href').extract()[0]
            items['features'] = release.xpath('div[@class="markdown-body"]/ul[position()=1]/li/text()').extract()
            items['fixes'] = release.xpath('div[@class="markdown-body"]/ul[position()=2]/li/text()').extract()

            yield items

        next_page = response.css('div.pagination a:nth-child(2)::attr(href)').get()
        is_first_page = response.css('div.pagination span::attr(text)').get() == 'Preview'

        if is_first_page:
            next_page = response.css('div.pagination a:nth-child(1)::attr(href)').get()

        if next_page is not None: 
            yield response.follow(next_page, callback=self.parse)