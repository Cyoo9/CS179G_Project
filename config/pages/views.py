from django import http
from django.shortcuts import render
from django.http import HttpResponse
import mysql.connector

## Connects to MySQL server 
'''
def index(request):
    db_connection = mysql.connector.connect(user="", password="")
    db_cursor = db_connection.cursor()
    db_cursor.execute("USE cs179g;")
    return HttpResponse(str(db_cursor.fetchall()))
    '''


##enter functions below

def homePageView(request):
    return render(request, 'index.html')

def hello(request):
    print('Hello from backend!!!')
    return HttpResponse('Hello World!')

def timeDifferences(request):
    #context
    db = mysql.connector.connect(user="caleb", password="password")
    cursor = db.cursor()
    cursor.execute("USE cs179g")
    issueTitle = request.GET.get('issueTitle')
    timeDiffLimit = request.GET.get('regLimit')
    if(issueTitle and timeDiffLimit):
        cursor.execute('SELECT * FROM TimeDifferences WHERE TimeDifferences.issue_titles = %s AND TimeDifferences.time_differences <= %s', (issueTitle, timeDiffLimit))
    elif(issueTitle and not timeDiffLimit):
        cursor.execute('SELECT * FROM TimeDifferences WHERE TimeDifferences.issue_titles = %s', (issueTitle,))
    elif(not issueTitle and timeDiffLimit):
        cursor.execute('SELECT * FROM TimeDifferences WHERE TimeDifferences.time_differences <= %s', (timeDiffLimit,))
    else:
        cursor.execute("SELECT * FROM TimeDifferences;")
    context = cursor.fetchall()
    cursor.close()
    return render(request, 'time_differences.html', {"data" : context})

def averageStatusTimeDifference(request):
    #add range for average time difference SELECT * FROM AverageTimeDifferences WHERE AverageTimeDifferences._2 > request.GET.
    db = mysql.connector.connect(user="caleb", password="password")
    cursor = db.cursor()
    cursor.execute("USE cs179g")
    issueStatus = request.GET.get('issueStatus')
    timeDiffLimit = request.GET.get('avgLimit')
    if(issueStatus and timeDiffLimit):
        cursor.execute('SELECT * FROM AverageTimeDifferences WHERE AverageTimeDifferences.issue_statuses = %s AND AverageTimeDifferences.avg_time_differences <= %s', (issueStatus, timeDiffLimit))
    elif(issueStatus and not timeDiffLimit):
        cursor.execute('SELECT * FROM AverageTimeDifferences WHERE AverageTimeDifferences.issue_statuses = %s', (issueStatus,))
    elif(not issueStatus and timeDiffLimit):
        cursor.execute('SELECT * FROM AverageTimeDifferences WHERE AverageTimeDifferences.avg_time_differences < %s', (timeDiffLimit,))
    else:
        cursor.execute("SELECT * FROM AverageTimeDifferences;")
    context = cursor.fetchall()
    cursor.close()
    return render(request, 'avg_time_differences.html', {"data" : context})

def timeDifferencesPieTable(request):
    #context
    db = mysql.connector.connect(user="caleb", password="password")
    cursor = db.cursor()
    cursor.execute("USE cs179g")
    issueTitle = request.GET.get('issueTitle')
    timeDiffLimit = request.GET.get('regLimit')
    if(issueTitle and timeDiffLimit):
        cursor.execute('SELECT * FROM TimeDifferences WHERE TimeDifferences.issue_titles = %s AND TimeDifferences.time_differences <= %s', (issueTitle, timeDiffLimit))
    elif(issueTitle and not timeDiffLimit):
        cursor.execute('SELECT * FROM TimeDifferences WHERE TimeDifferences.issue_titles = %s', (issueTitle,))
    elif(not issueTitle and timeDiffLimit):
        cursor.execute('SELECT * FROM TimeDifferences WHERE TimeDifferences.time_differences <= %s', (timeDiffLimit,))
    else:
        cursor.execute("SELECT * FROM TimeDifferences;")
    context = cursor.fetchall()
    cursor.close()
    return render(request, 'time_differences_pie_table.html', {"data" : context})

def averageStatusTimeDifferencePieTable(request):
    #add range for average time difference SELECT * FROM AverageTimeDifferences WHERE AverageTimeDifferences._2 > request.GET.
    db = mysql.connector.connect(user="caleb", password="password")
    cursor = db.cursor()
    cursor.execute("USE cs179g")
    issueStatus = request.GET.get('issueStatus')
    timeDiffLimit = request.GET.get('avgLimit')
    if(issueStatus and timeDiffLimit):
        cursor.execute('SELECT * FROM AverageTimeDifferences WHERE AverageTimeDifferences.issue_statuses = %s AND AverageTimeDifferences.avg_time_differences <= %s', (issueStatus, timeDiffLimit))
    elif(issueStatus and not timeDiffLimit):
        cursor.execute('SELECT * FROM AverageTimeDifferences WHERE AverageTimeDifferences.issue_statuses = %s', (issueStatus,))
    elif(not issueStatus and timeDiffLimit):
        cursor.execute('SELECT * FROM AverageTimeDifferences WHERE AverageTimeDifferences.avg_time_differences < %s', (timeDiffLimit,))
    else:
        cursor.execute("SELECT * FROM AverageTimeDifferences;")
    context = cursor.fetchall()
    cursor.close()
    return render(request, 'avg_time_differences_pie_table.html', {"data" : context})

