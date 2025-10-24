from django.contrib import admin
from .models import PerformanceMetric

@admin.register(PerformanceMetric)
class PerformanceMetricAdmin(admin.ModelAdmin):
    list_display = ('district', 'year', 'total_work_days', 'completed_work_days', 'expenditure')
    search_fields = ('district__name',)
    list_filter = ('year',)