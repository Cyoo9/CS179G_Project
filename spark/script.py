import pyspark
from pyspark.sql import SQLContext

sparkCont = pyspark.SparkContext('local[*]')
sc = SQLContext(sparkCont)

issues = sc.read.option("multiline", "true").json("issues_filtered.json")
issues.createOrReplaceTempView("issues_filtered_json")

releases = sc.read.option("multiline", "true").json("./cs179g_crawler/releases.json")
releases.createOrReplaceTempView("releases_json")
df = sc.sql("SELECT * from releases_json")

release_times = df.select('date').collect() 
linked_pull_requests = df.select('pull_request_ids').collect()

pull_requests = sc.read.option("multiline", "true").json("pull_requests_filtered.json")
pull_requests.createOrReplaceTempView("pull_requests_json")
df = sc.sql("SELECT * from pull_requests_json")
pull_request_ids = df.select('id').collect()
linked_issues = df.select('linked_issue').collect()
issue_times = df.select('date').collect()

issues = sc.read.option("multiline", "true").json("issues_filtered.json")
pull_requests.createOrReplaceTempView("issues_json")
df = sc.sql("SELECT * from issues_json")
issue_titles = df.select('title').collect()

print(len(pull_request_ids))
print(len(linked_issues))
print(len(issue_titles))

# First check if pull request is part of a release and get the the time of release 

issue_resolved_differences = []

for pull_request in pull_request_ids:
    releaseDate = ""
    issueDate = ""
    if(len(pull_request.id)):
        for releaseTime in release_times:
            for linked_pull_request in linked_pull_requests:
                if pull_request.id[0] in linked_pull_request.pull_request_ids:
                    releaseDate = releaseTime #find release time based on matching pull requests. 
        # for issueTime in issue_times:

                    
                
#Then, match the pull request's linked_issue to an issue title to get the time of the issue (need to merge this with release date/time somehow)

# for issue in issue_titles:
#     for linked_issue in linked_issues:
#         if(len(linked_issue[0]) > 1):
#             print(linked_issue[0][1])
