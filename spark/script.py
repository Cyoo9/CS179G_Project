# from datetime import time
# import pyspark
# from pyspark.sql import SQLContext
# import dateutil.parser
# import mysql.connector


# sparkCont = pyspark.SparkContext('local[*]')
# sc = SQLContext(sparkCont)

# releases = sc.read.option("multiline", "true").json("./cs179g_crawler/releases.json")
# releases.createOrReplaceTempView("releases_json")
# df = sc.sql("SELECT * from releases_json")
# releases_info = df.select('tag', 'features_and_fixes', 'pull_request_ids', 'date').collect()

# pull_requests = sc.read.option("multiline", "true").json("pull_requests_filtered.json")
# pull_requests.createOrReplaceTempView("pull_requests_json")
# df = sc.sql("SELECT * from pull_requests_json")
# pull_requests_info = df.select('id', 'linked_issue', 'title').collect() #need to merge this with linked_issues, and issue_times

# issues = sc.read.option("multiline", "true").json("issues_filtered.json")
# issues.createOrReplaceTempView("issues_json")
# df = sc.sql("SELECT * from issues_json")
# issues_info = df.select('title', 'url', 'date', 'status').collect() #need to merge this with other issue fields

# db_connection = mysql.connector.connect(user="ckong", password="cs179g")
# db_cursor = db_connection.cursor(buffered=True)
# db_cursor.execute("USE cs179g;")
# db_cursor.execute("CREATE TABLE IF NOT EXISTS TimeDifferences(\
#     issue_titles TEXT, \
#     issue_statuses TEXT, \
#     release_features_and_fixes TEXT, \
#     time_differences FLOAT,\
#     release_tags TEXT);")

# row = { "issue_titles": [], "issue_statuses": [], "release_features_and_fixes": [], "time_differences": [], "release_tags": [] }

# for request in pull_requests_info: #loop through pull requests
#     issueDate = ""
#     releaseDate = ""
#     timeDifference = 0.0
#     issue_title = ""
#     issue_status = ""
#     for issue in issues_info: #loop through issues
#         if(len(request.linked_issue) > 1): #check if linked issue is not empty
#             if(issue.title == request.linked_issue[1]): #if it is not empty, check if the issue title matches one of the linked issues 
#                 issueDate = dateutil.parser.parse(issue.date).timestamp()
#                 issue_title = issue.title
#                 issue_status = issue.status
#                 # print(issueDate)
#                 break #leave issue for loop. we are done checking for issues until next pull request. 
#     if(issueDate): #if we had a matching issue above, loop through releases to get release date
#         for release in releases_info:
#             if(len(request.id) > 0):
#                 if(request.id[0] in release.pull_request_ids):
#                     releaseDate = dateutil.parser.parse(release.date).timestamp()
#                     timeDifference = releaseDate - issueDate
                    
#                     row['issue_titles'] = issue_title
#                     row['issue_statuses'] = issue_status
#                     row['issue_statuses'] = '' # need to concatenate issue_statuses indices to store it as 1 string
#                     for i in range(len(issue_status)):
#                         row['issue_statuses'] += ' ' + issue_status[i]
                    
#                     row['release_features_and_fixes'] = '' # need to concatenate features_and_fixes indices to store it as 1 string
#                     for i in range(len(release.features_and_fixes)):
#                         row['release_features_and_fixes'] += release.features_and_fixes[i]
                        
#                     row['time_differences'] = timeDifference
#                     row['release_tags'] = release.tag
                                        
#                     query = "INSERT INTO TimeDifferences VALUES (%s, %s, %s, %s, %s);"
#                     data = (row['issue_titles'], row['issue_statuses'], row['release_features_and_fixes'], row['time_differences'], row['release_tags'])

#                     db_cursor.execute(query, data)
#                     db_cursor.execute("FLUSH TABLES;")
#                     break #leave release for loop. we are done checking for releases until next pull request. we have our time difference.

# #store row in tuple so we can compute avg time difference using spark
# print(row)

# avg_time_diff = zip(row['issue_statuses'], row['time_differences'])
#     avg_time_diff.append(status, time_diff)

# print("avg time differences: \n")
# print(avg_time_diff)


# store data in mysql


# print('Finished inserting data into MySQL')
                    
# db_cursor.execute("SELECT * FROM TimeDifferences;")
# records = db_cursor.fetchall()
# # print(db_cursor.fetchall())

# for record in records:
#         print("issue_titles = ", record[0])
#         print("issue_statuses = ", record[1])
#         print("release_features_and_fixes = ", record[2])
#         print("time_differences  = ", record[3])
#         print("release_tags  = ", record[4], "\n")

# print(processed_data)
# print(len(processed_data['issue_titles']))
# print(len(processed_data['issue_statuses']))
# print(len(processed_data['release_features_and_fixes']))
# print(len(processed_data['time_differences']))
# print(len(processed_data['release_tags']))

from datetime import time
import pyspark
from pyspark.sql import SQLContext
import dateutil.parser
import time

startTime = time.time()

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
                if(issue.status):
                    issue_status = issue.status[0]
                else:
                    issue_status = ""
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
                
status_time_diff = sparkCont.parallelize(list(zip(processed_data['issue_statuses'], processed_data['time_differences'])))
print("all time diff: ")
print(status_time_diff.collect())

status_frequencies = []
for status in processed_data["issue_statuses"]:
    status_frequencies.append((status, 1))

status_frequencies_parallelized = sparkCont.parallelize(status_frequencies)

status_time_diff_total = status_time_diff.reduceByKey(lambda x, y: x + y)
status_frequencies_total = status_frequencies_parallelized.reduceByKey(lambda x, y: x + y)

avg_time_diff = status_time_diff_total.join(status_frequencies_total).mapValues(lambda x: x[0] / x[1])

print("avg time diffs for each status: ")
print(avg_time_diff.collect())

endTime = time.time()

print("total execution time: ", endTime - startTime)





