# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class QuestionItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    date_posted = scrapy.Field() 
    pass


class IssuesItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field() 
    url = scrapy.Field()
    pass

class ReleasesItem(scrapy.Item): 
    tag = scrapy.Field()
    url = scrapy.Field()
    features = scrapy.Field()
    fixes = scrapy.Field()

    pass 