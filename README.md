# CS179G_Project
Big data analytics project using Spark

# After cloning
- git branch *your-branch-name*
- git checkout *your-branch-name*
- git push --set-upstream origin *your-branch-name*

# <spider_name>.py 
- Use spider to crawl data 

# pipelines.py 
- Use Spark to store crawled data inside MySQL database 

# execute 
- scrapy crawl stack_spider (stackoverflow questions)
- scrapy crawl releases_spider (github releases spider. using vue storefront repo)
- scrapy crawl issues_spider (github issues spider. using vue storefront repo) 

# output files
- items.json / items.csv (stackoverflow live questions)
- issues.json (github issues)
- releases.json (github releases)
