from django.urls import path
from django.urls.resolvers import URLPattern

from . import views

urlpatterns = [
    path('', views.homePageView, name='index'),
    path('getTimeDifferencesGraph', views.timeDifferences, name='timeDifferences'),
    path('getTimeDifferencesGraphPieTable', views.timeDifferencesPieTable, name='timeDifferencesPieTable'),
    path('getAvgStatusTimeDifferencesGraphPieTable', views.averageStatusTimeDifferencePieTable, name='averageStatusTimeDifferencePieTable'),
    path('getAvgStatusTimeDifferencesGraph', views.averageStatusTimeDifference, name='averageStatusTimeDifference')
]
