# CS179G_Project
Big data analytics project using Python Spark and MySQL DB

# <spider_name>.py 
- Use spider to crawl data 
- Store data in json / csv 

# pipelines.py 
- Use Spark to parse json / csv file (>=2GB) and store data inside MySQL database ( uncomment this line: ITEM_PIPELINES = {
    'cs179g_crawler.pipelines.Cs179GCrawlerPipeline': 300,} inside settings.py ).

# execute 
- scrapy crawl <spider_name> 
