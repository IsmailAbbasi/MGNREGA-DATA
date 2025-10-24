from django.urls import path
from . import views

urlpatterns = [
    path('', views.performance_dashboard, name='performance_dashboard'),
    path('district/<int:district_id>/', views.district_performance, name='district_performance'),
]