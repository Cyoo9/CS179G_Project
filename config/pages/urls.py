from django.urls import path
from django.urls.resolvers import URLPattern

from . import views

urlpatterns = [
    path('', views.homePageView, name='index'),
    path('actionUrl', views.hello, name='hello'),
    path('getTimeDifferencesGraph', views.timeDifferences, name='timeDifferences')
]
