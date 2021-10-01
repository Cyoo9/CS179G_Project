import scrapy
from ..items import QuoteItem 

class QuoteSpider(scrapy.Spider):
    name = 'quotes' 
    start_urls = [
        'http://quotes.toscrape.com/'
    ]

    def parse(self, response): 
        items = QuoteItem()
        allQuotes = response.css('div.quote')
        
        for quote in allQuotes:
            title = quote.css('span.text::text').extract()
            author = quote.css('.author::text').extract()
            tag = quote.css('.tag::text').extract()

            items['title'] = title
            items['author'] = author
            items['tag'] = tag

            yield items