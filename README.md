# CS179G_Project
Big data analytics project using Spark

# stack_spider.py 
- Use spider to crawl new stack overflow questions.  

# pipelines.py 
- Use pyspark to store crawled data into MySQL / NoSQL database.

# execute 
- scrapy crawl stack (if you just want to crawl)
- scrapy crawl stack -o items.csv -t csv (if you want to store scraped data into csv file)
- scrapy crawl stack -o items.json -t json (if you want to store scraped data ino json file) 

# issues / questions
- crawling is too slow right now to crawl at least 2GB of data (5 seconds per page). 
- can json/csv file represent the 2gb of data storage we have before it's parsed into sql db?

