# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class IssuesItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field() 
    url = scrapy.Field()
    date = scrapy.Field()
    status = scrapy.Field()
    pass

class PullRequestsItem(scrapy.Item):
    title = scrapy.Field()
    id = scrapy.Field()
    linked_issue = scrapy.Field()
    pass


class ReleasesItem(scrapy.Item): 
    tag = scrapy.Field()
    url = scrapy.Field()
    pull_request_ids = scrapy.Field() 
    features_and_fixes = scrapy.Field()
    date = scrapy.Field()
    pass 