from datetime import time
import pyspark
from pyspark.sql import SQLContext
import dateutil.parser

#imports for mysql and pyspark
import pandas as pd
import mysql.connector
from pyspark.sql import SparkSession
#------------------------------

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

processed_data = { "issue_titles": [], "release_features_and_fixes": [], "time_differerences": [] } #this goes into mysql?

#Testing Mysql database connection
print('#########')

db_connection = mysql.connector.connect(user="jnguy557", password="password")
db_cursor = db_connection.cursor()
db_cursor.execute("USE cs179g;")
#create a table using processed data columns




'''
db_cursor.execute("CREATE TABLE IF NOT EXISTS Wines(fixed_acidity FLOAT, 
volatile_acidity FLOAT, \
                   citric_acid FLOAT, residual_sugar FLOAT, chlorides FLOAT, \
                   free_so2 FLOAT, total_so2 FLOAT, density FLOAT, pH FLOAT, \
                   sulphates FLOAT, alcohol FLOAT, quality INT, is_red INT);")
wine_tuples = list(all_wines.itertuples(index=False, name=None))
wine_tuples_string = ",".join(["(" + ",".join([str(w) for w in wt]) + ")" for wt in
wine_tuples])
db_cursor.execute("INSERT INTO Wines(fixed_acidity, volatile_acidity, citric_acid,\
                   residual_sugar, chlorides, free_so2, total_so2, density, pH,\
                   sulphates, alcohol, quality, is_red) VALUES " + 
wine_tuples_string + ";")
db_cursor.execute("FLUSH TABLES;")
db_cursor.execute("SELECT * FROM Wines LIMIT 5;")
print(db_cursor.fetchall())
# Test connection to MySQL via PySpark
# /usr/share/java/mysql-connector-java-8.0.26.jar is from mysql-connector-java
spark = SparkSession.builder.config("spark.jars", "/usr/share/java/mysql-connector-
java-8.0.26.jar") \
    .master("local").appName("PySpark_MySQL_test").getOrCreate()
wine_df = spark.read.format("jdbc").option("url", 
"jdbc:mysql://localhost:3306/cs179g") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "Wines") \
    .option("user", "your_name").option("password", "some_password").load()
print(wine_df)

'''

print('$$$$$$$$$$$')
# for request in pull_requests_info: #loop through pull requests
#     issueDate = ""
#     releaseDate = ""
#     timeDifference = ""
#     for issue in issues_info: #loop through issues
#         if(len(request.linked_issue) > 1): #check if linked issue is not epmty
#             if(issue.title == request.linked_issue[1]): #if it is not empty, check if the issue title matches one of the linked issues 
#                 issueDate = dateutil.parser.parse(issue.date).timestamp()
#                 processed_data["issue_titles"].append(issue.title)
#                 #print(issueDate)
#                 break #leave issue for loop. we are done checking for issues until next pull request. 
#     if(issueDate): #if we had a matching issue above, loop through releases to get release date
#         for release in releases_info:
#             if(len(request.id) > 0):
#                 if(request.id[0] in release.pull_request_ids):
#                     releaseDate = dateutil.parser.parse(release.date).timestamp()
#                     timeDifference = releaseDate - issueDate
#                     processed_data["release_features_and_fixes"].append(release.features_and_fixes)
#                     processed_data["time_differerences"].append(timeDifference)
#                     break #leave release for loop. we are done checking for releases until next pull request. we have our time difference. 
