
import pyspark
from pyspark.sql import SQLContext



sparkCont = pyspark.SparkContext('local[*]')
sc = SQLContext(sparkCont)


issues = sc.read.option("multiline", "true").json("issues_filtered.json")
issues.createOrReplaceTempView("issues_filtered_json")
#sc.sql("SELECT * FROM issues_filtered_json").show()

releases = sc.read.option("multiline", "true").json("./cs179g_crawler/releases.json")
releases.createOrReplaceTempView("releases_json")
df = sc.sql("SELECT * from releases_json")
release_list = df.select('pull_request_ids').collect()
#sc.sql("SELECT * FROM releases_json").show()
'''for release in release_list:
    for item in release:
        for temp in item:
            print(temp)''' #prints out the pull request release ID

pull_requests = sc.read.option("multiline", "true").json("pull_requests_filtered.json")
pull_requests.createOrReplaceTempView("pull_requests_json")
df = sc.sql("SELECT * from pull_requests_json")
pull_list = df.select('id').collect()


##only outputs 25 ids, need to figure away to bypass that limit
for pull in pull_list:
    print(pull.id[0])





'''
#issues = sc.read.json('issues_filtered.json')

releases = sc.read.json('cs179g_crawler/releases.json')
releases.createOrReplaceTempView("pull_request_ids")

pull_requests = sc.read.json('cs179g_crawler/pull_requests.json')
pull_requests.createOrReplaceTempView("linked_issue")


#issues = sc.read.json('issues_filtered.json')


releases = sc.read.json('cs179g_crawler/releases.json')
releases.createOrReplaceTempView("pull_request_ids")

pull_requests = sc.read.json('cs179g_crawler/pull_requests.json')
pull_requests.createOrReplaceTempView("linked_issue")'''



#match pull requests linked issue to issues_filtered title. match pull request id to ids in releases. 
#if matched, compare releases date and issue date and take difference to find out how long it took for issue to be resolved. 

