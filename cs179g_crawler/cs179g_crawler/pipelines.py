# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import mysql.connector 
from pyspark.sql import SparkSession

class Cs179GCrawlerPipeline(object):

    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        self.conn = mysql.connector.connect(
            user = 'caleb', 
            password = 'password',
        )
        self.curr = self.conn.cursor()
    
    def create_table(self):
        self.curr.execute("""DROP TABLE IF EXISTS questions_tb""")
        self.curr.execute("""create table questions_tb(
            title text,
            url text,
            date_posted text
        )""")

    def process_item(self, item):
        self.store_db(item)
        return item

    def store_db(self, item):
        self.curr.execute("""insert into questions_tb values(%s, %s, %s)""",
        (
            item['title'][0],
            item['url'][0],
            item['date_posted'][0]
        ))
        self.conn.commit() 
