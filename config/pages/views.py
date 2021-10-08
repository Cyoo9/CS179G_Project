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

