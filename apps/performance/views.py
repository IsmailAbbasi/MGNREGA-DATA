from django.shortcuts import render
from django.http import JsonResponse
from .models import PerformanceMetric
from apps.districts.models import District

def performance_dashboard(request):
    districts = District.objects.all()
    selected_district = request.GET.get('district')
    performance_data = None

    if selected_district:
        performance_data = PerformanceMetric.objects.filter(district__id=selected_district)

    context = {
        'districts': districts,
        'performance_data': performance_data,
        'selected_district': selected_district,
    }
    
    return render(request, 'performance/performance_dashboard.html', context)

def district_performance(request, district_id):
    data = {
        "district_id": district_id,
        "status": "ok",
        "message": f"Placeholder performance data for district {district_id}"
    }
    return JsonResponse(data)