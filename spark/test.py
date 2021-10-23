import pyspark
from pyspark.sql import SQLContext

sparkCont = pyspark.SparkContext('local[*]')
sc = SQLContext(sparkCont)


issues = sc.read.json('issues_filtered.json')
issues.createOrReplaceTempView("title")

releases = sc.read.json('cs179g_crawler/releases.json')
releases.createOrReplaceTempView("pull_request_ids")

pull_requests = sc.read.json('cs179g_crawler/pull_requests.json')
pull_requests.createOrReplaceTempView("linked_issue")

sc.sql("SELECT * FROM title").show()




#match pull requests linked issue to issues_filtered title. match pull request id to ids in releases. 
#if matched, compare releases date and issue date and take difference to find out how long it took for issue to be resolved. 
