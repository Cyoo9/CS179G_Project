# CS179G_Project
Big data analytics project using Python Spark and MySQL DB

# stack_spider.py 
- Use spider to crawl new stack overflow questions  
- Store data in json 

# pipelines.py 
- Use Spark to parse json / csv file (>=2GB) and store data inside MySQL database ( uncomment this line: ITEM_PIPELINES = {
    'cs179g_crawler.pipelines.Cs179GCrawlerPipeline': 300,} inside settings.py ).

# execute 
- scrapy crawl stack (if you just want to crawl)
- scrapy crawl stack -o items.csv -t csv (if you want to store scraped data into csv file)
- scrapy crawl stack -o items.json -t json (if you want to store scraped data ino json file) 

# issues / questions
- crawling is too slow right now to crawl at least 2GB of data (5 seconds per page). 
- can json/csv file represent the 2gb of data storage we have before it's parsed into sql db?

