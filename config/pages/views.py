from django.shortcuts import render
from django.http import HttpResponse

##enter functions below

def homePageView(request):
    return render(request, 'index.html')

def hello(request):
    print('Hello from backend!!!')
    return HttpResponse('<!DOCTYPE html> <html><head><title>CS179G Project</title></head> <body> <h1>Github Analysis</h1><hr/> <button type=“button”> Click Me</button><h1 id="hello">Hello World!</h1></body> </html> ')


