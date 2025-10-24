from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', RedirectView.as_view(url='/districts/', permanent=False)),
    path('districts/', include('apps.districts.urls')),
    path('performance/', include('apps.performance.urls')),
]