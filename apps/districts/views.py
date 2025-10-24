from django.shortcuts import render, get_object_or_404
from .models import District, MGNREGAData

def district_list(request):
    districts = District.objects.all().order_by('name')
    return render(request, 'districts/district_list.html', {'districts': districts})

def district_detail(request, district_id):
    district = get_object_or_404(District, id=district_id)
    
    # Get historical data
    mgnrega_data = MGNREGAData.objects.filter(district=district).order_by('-year')
    
    # Calculate aggregates
    if mgnrega_data.exists():
        latest = mgnrega_data.first()
        total_expenditure = sum(d.total_expenditure for d in mgnrega_data)
        avg_employment_rate = sum(d.employment_rate for d in mgnrega_data) / mgnrega_data.count()
    else:
        latest = None
        total_expenditure = 0
        avg_employment_rate = 0
    
    context = {
        'district': district,
        'latest_data': latest,
        'historical_data': mgnrega_data[:5],  # Last 5 years
        'total_expenditure': total_expenditure,
        'avg_employment_rate': avg_employment_rate,
    }
    
    return render(request, 'districts/district_detail.html', context)
