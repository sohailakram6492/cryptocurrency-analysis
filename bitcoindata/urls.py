from django.urls import path
from . import views



urlpatterns = [
    # path('', views, name='index'),
    
    path('bitcoinrealtime', views.bitcoinrealtime, name='bitcoinrealtime'),
    
]