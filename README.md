# CS179G_Project
Big data analytics project using Python Spark and MySQL DB

# <spider_name>.py 
- Use spider to crawl data 

# pipelines.py 
- Use Spark to store crawled data inside MySQL / NoSQL database ( uncomment this line: ITEM_PIPELINES = {
    'cs179g_crawler.pipelines.Cs179GCrawlerPipeline': 300,} inside settings.py ).

# execute 
- scrapy crawl <spider_name> 
