from django.contrib import admin
from .models import District, MGNREGAData

@admin.register(District)
class DistrictAdmin(admin.ModelAdmin):
    list_display = ('name', 'state', 'district_code', 'population', 'get_performance_score')
    search_fields = ('name', 'state', 'district_code')
    list_filter = ('state',)
    
    def get_performance_score(self, obj):
        """Calculate performance score from latest MGNREGA data"""
        latest_data = MGNREGAData.objects.filter(district=obj).order_by('-year', '-month').first()
        if latest_data:
            return f"{latest_data.employment_rate:.1f}%"
        return "N/A"
    
    get_performance_score.short_description = 'Performance Score'

@admin.register(MGNREGAData)
class MGNREGADataAdmin(admin.ModelAdmin):
    list_display = ('district', 'year', 'month', 'employment_rate', 'total_workers', 'total_expenditure')
    list_filter = ('year', 'month', 'district__state')
    search_fields = ('district__name',)
    ordering = ('-year', '-month')