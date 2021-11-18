from datetime import time
from mysql.connector import connection
import pyspark
from pyspark.sql import SQLContext
import dateutil.parser
import time
import mysql.connector
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
import json

startTime = time.time()

sparkCont = pyspark.SparkContext('local[*]')
sc = SQLContext(sparkCont)

releases = sc.read.option("multiline", "true").json("./cs179g_crawler/releases.json")
releases.createOrReplaceTempView("releases_json")
df = sc.sql("SELECT * from releases_json")
releases_info = sparkCont.parallelize(df.select('tag', 'features_and_fixes', 'pull_request_ids', 'date').collect())
releases_infoCopy = df.select('tag', 'features_and_fixes', 'pull_request_ids', 'date').collect() #need to merge this with linked_issues, and issue_times

pull_requests = sc.read.option("multiline", "true").json("pull_requests_filtered.json")
pull_requests.createOrReplaceTempView("pull_requests_json")
df = sc.sql("SELECT * from pull_requests_json")
pull_requests_info = sparkCont.parallelize(df.select('id', 'linked_issue', 'title').collect()) #need to merge this with linked_issues, and issue_times

issues = sc.read.option("multiline", "true").json("issues_filtered.json")
issues.createOrReplaceTempView("issues_json")
df = sc.sql("SELECT * from issues_json")
issues_info = sparkCont.parallelize(df.select('issue_title', 'url', 'issue_date', 'status').collect()) #need to merge this with other issue fields

issues_infoCopy = df.select('issue_title', 'url', 'issue_date', 'status').collect() #need to merge this with other issue fields

# make the table
db_connection = mysql.connector.connect(
  host="localhost",
  user="caleb",
  password="password",
  database="cs179g",
  auth_plugin='mysql_native_password'
)


db_cursor = db_connection.cursor(buffered=True)
db_cursor.execute("USE cs179g;")
db_cursor.execute(" DROP TABLE IF EXISTS TimeDifferences; \
    CREATE TABLE TimeDifferences(\
    issue_titles TEXT, \
    issue_statuses TEXT, \
    release_features_and_fixes TEXT, \
    time_differences FLOAT,\
    release_tags TEXT);")

prDF = pull_requests_info.toDF()
issueDF = issues_info.toDF()
releaseDF = releases_info.toDF()

desiredPRIssue = prDF.join(issueDF, prDF.linked_issue[1] == issueDF.issue_title, how="inner")

desiredReleasePRIssue = desiredPRIssue.join(releaseDF, releaseDF.pull_request_ids.cast("string").contains(desiredPRIssue.id[0]), how="inner")

processed_data = { "issue_titles": [], "issue_statuses": [], "release_features_and_fixes": [], "time_differences": [], "release_tags": [] } 
row = { "issue_titles": [], "issue_statuses": [], "release_features_and_fixes": [], "time_differences": [], "release_tags": [] } #goes into mysql. con

for issueAndRelease in desiredReleasePRIssue.collect():
  if(issueAndRelease.id[0] in issueAndRelease.pull_request_ids):
    timeDiff = (dateutil.parser.parse(issueAndRelease.date).timestamp() - dateutil.parser.parse(issueAndRelease.issue_date).timestamp()) / 86400

    processed_data['issue_titles'].append(issueAndRelease.issue_title)

    status = ''
    for i in range(len(issueAndRelease.status)):
      status += ' ' + issueAndRelease.status[i]

    processed_data['issue_statuses'].append(status)

    release_features_and_fixes = ''
    for i in range(len(issueAndRelease.features_and_fixes)):
      release_features_and_fixes += issueAndRelease.features_and_fixes[i]

    processed_data['release_features_and_fixes'].append(release_features_and_fixes)
    processed_data['time_differences'].append(timeDiff)
    processed_data['release_tags'].append(issueAndRelease.tag)
                      
    query = "INSERT INTO TimeDifferences VALUES (%s, %s, %s, %s, %s);"
    data = (processed_data['issue_titles'][-1], processed_data['issue_statuses'][-1], processed_data['release_features_and_fixes'][-1], processed_data['time_differences'][-1], processed_data['release_tags'][-1])
    db_cursor.close()

    db_connection = mysql.connector.connect(
        host="localhost",
        user="caleb",
        password="password",
        database="cs179g",
        auth_plugin='mysql_native_password'
    )

    db_cursor = db_connection.cursor(buffered=True)
    db_cursor.execute("USE cs179g;")
    db_cursor.execute(query, data) 
    db_cursor.execute("FLUSH TABLES;")
    db_cursor.close()


# with open("test.json", "w") as outfile:
#   outfile.write(json.dumps(finalResult))

status_time_diff = sparkCont.parallelize(list(zip(processed_data['issue_statuses'], processed_data['time_differences'])))

print(status_time_diff.collect())
status_frequencies = []
for status in processed_data["issue_statuses"]:
    status_frequencies.append((status, 1))

status_frequencies_parallelized = sparkCont.parallelize(status_frequencies)
status_time_diff_total = status_time_diff.reduceByKey(lambda x, y: x + y)
status_frequencies_total = status_frequencies_parallelized.reduceByKey(lambda x, y: x + y)

avg_time_diff = status_time_diff_total.join(status_frequencies_total).mapValues(lambda x: x[0] / x[1])

#storing avg_time_diff into database table named AverageTimeDifferences
avg_time_diff_df = sc.createDataFrame(avg_time_diff)
avg_time_diff_df = avg_time_diff_df.toPandas()
engine = create_engine("mysql+pymysql://caleb:password@localhost/cs179g")
avg_time_diff_df.to_sql(con=engine, name='AverageTimeDifferences', if_exists='replace', index=False)

print("avg time diffs for each status: ")
print(avg_time_diff.collect())

endTime = time.time()

print("total execution time: ", endTime - startTime)


