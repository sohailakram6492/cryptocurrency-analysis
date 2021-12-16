from django.urls import path
from . import views
from django.conf.urls.static import static
from .views import image_view
from django.conf import settings



urlpatterns = [
    # path('', views, name='index'),
    
    path('', views.indexmain, name='about-us'),
    path('about-us/', views.about, name='about-us'),
    path('signup', views.signup, name="signup"),
    path("login", views.login, name="login"),
    path("logout", views.logout, name= "logout"),
    path("main", views.Main, name= "main"),
    path("ethereum", views.ethereum, name= "ethereum"),
    path("bitcoin", views.Index.as_view(), name="bitcoin"),
    path("eth", views.Eth.as_view(), name="eth"),
    path("ada", views.ada, name= "ada"),
    path("bitcoinlive", views.chart, name="chart"),
    path("bitcoinrealtime", views.bitcoinrealtime, name="bitcoinrealtime"),
    path("facerecognition", views.facerecognition, name="facerecognition"),
    path("video_feed", views.video_feed, name="video_feed"),
    path('image', image_view, name = 'image'),
  
    
]

if settings.DEBUG:
        urlpatterns += static(settings.MEDIA_URL,
                              document_root=settings.MEDIA_ROOT)



