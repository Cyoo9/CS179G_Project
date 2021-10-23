import pyspark

sc = pyspark.SparkContext('local[*]')
issues = sc.textFile('issues_filtered.json')
issueCount = issues.flatMap(lambda line: line.split(","))
print(issueCount.take(100))

releases = sc.textFile('cs179g_crawler/releases.json')
pull_requests = sc.textFile('cs179g_crawler/pull_requests.json')

print(releases.take(100))
print(pull_requests.take(100))

#match pull requests linked issue to issues_filtered title. match pull request id to ids in releases. 
#if matched, compare releases date and issue date and take difference to find out how long it took for issue to be resolved. 
