from datetime import time
import pyspark
from pyspark.sql import SQLContext
import dateutil.parser

sparkCont = pyspark.SparkContext('local[*]')
sc = SQLContext(sparkCont)

releases = sc.read.option("multiline", "true").json("./cs179g_crawler/releases.json")
releases.createOrReplaceTempView("releases_json")
df = sc.sql("SELECT * from releases_json")
releases_info = df.select('tag', 'features_and_fixes', 'pull_request_ids', 'date').collect()

pull_requests = sc.read.option("multiline", "true").json("pull_requests_filtered.json")
pull_requests.createOrReplaceTempView("pull_requests_json")
df = sc.sql("SELECT * from pull_requests_json")
pull_requests_info = df.select('id', 'linked_issue', 'title').collect() #need to merge this with linked_issues, and issue_times

issues = sc.read.option("multiline", "true").json("issues_filtered.json")
issues.createOrReplaceTempView("issues_json")
df = sc.sql("SELECT * from issues_json")
issues_info = df.select('title', 'url', 'date', 'status').collect() #need to merge this with other issue fields

processed_data = { "issue_titles": [], "issue_statuses": [], "release_features_and_fixes": [], "time_differences": [], "release_tags": [] } #this goes into mysql?

for request in pull_requests_info: #loop through pull requests
    issueDate = ""
    releaseDate = ""
    timeDifference = ""
    issue_title = ""
    issue_status = ""
    for issue in issues_info: #loop through issues
        if(len(request.linked_issue) > 1): #check if linked issue is not epmty
            if(issue.title == request.linked_issue[1]): #if it is not empty, check if the issue title matches one of the linked issues 
                issueDate = dateutil.parser.parse(issue.date).timestamp()
                issue_title = issue.title
                issue_status = issue.status
                # print(issueDate)
                break #leave issue for loop. we are done checking for issues until next pull request. 
    if(issueDate): #if we had a matching issue above, loop through releases to get release date
        for release in releases_info:
            if(len(request.id) > 0):
                if(request.id[0] in release.pull_request_ids):
                    releaseDate = dateutil.parser.parse(release.date).timestamp()
                    timeDifference = releaseDate - issueDate
                    processed_data["release_features_and_fixes"].append(release.features_and_fixes)
                    processed_data["time_differences"].append(timeDifference)
                    processed_data["release_tags"].append(release.tag)
                    processed_data["issue_titles"].append(issue_title)
                    processed_data["issue_statuses"].append(issue_status)
                    break #leave release for loop. we are done checking for releases until next pull request. we have our time difference. 

print(processed_data)
print(len(processed_data['issue_titles']))
print(len(processed_data['issue_statuses']))
print(len(processed_data['release_features_and_fixes']))
print(len(processed_data['time_differences']))
print(len(processed_data['release_tags']))

