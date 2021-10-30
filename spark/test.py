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

print(len(pull_list))
for pull in pull_list:
    if(len(pull.id)):
        print(pull.id[0])