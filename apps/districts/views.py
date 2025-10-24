from django.shortcuts import render, get_object_or_404
from django.db.models import Count, Q
from .models import District, MGNREGAData
from apps.performance.services import MGNREGADataService
import logging

logger = logging.getLogger(__name__)

def district_list(request):
    """Display all districts with data availability indicator"""
    # Get all districts with data count
    districts = District.objects.annotate(
        data_count=Count('mgnregadata')
    ).order_by('state', 'name')
    
    # Count districts with data
    districts_with_data = districts.filter(data_count__gt=0)
    districts_with_data_count = districts_with_data.count()
    total_districts = districts.count()
    
    # Get popular districts (only those with data)
    popular_names = ['Bengaluru', 'Lucknow', 'Pune', 'Jaipur', 'Ahmadabad', 
                     'Nagpur', 'Patna', 'Bhopal', 'Indore', 'Kanpur']
    popular_districts = districts_with_data.filter(name__in=popular_names)
    
    context = {
        'districts': districts,
        'popular_districts': popular_districts,
        'districts_with_data_count': districts_with_data_count,
        'total_districts': total_districts,
    }
    
    return render(request, 'districts/district_list.html', context)

def district_detail(request, district_id):
    """Display MGNREGA performance data for a specific district"""
    district = get_object_or_404(District, id=district_id)
    
    # Get data from database
    mgnrega_data = MGNREGAData.objects.filter(district=district).order_by('-year', '-month')
    
    # Try to refresh from API in background (non-blocking)
    data_source = 'Database (cached)'
    try:
        api_data = MGNREGADataService.fetch_district_data(district.district_code)
        if api_data and api_data.get('source') == 'api':
            data_source = 'Live from Government API'
            # Re-query after potential API sync
            mgnrega_data = MGNREGAData.objects.filter(district=district).order_by('-year', '-month')
    except Exception as e:
        logger.warning(f"API refresh failed for {district.name}: {e}")
    
    # Prepare context
    if mgnrega_data.exists():
        latest = mgnrega_data.first()
        
        # Calculate aggregates safely
        total_expenditure = sum(float(d.total_expenditure or 0) for d in mgnrega_data)
        total_workers = sum(int(d.total_workers or 0) for d in mgnrega_data)
        total_work_days = sum(float(d.total_work_days or 0) for d in mgnrega_data)
        
        # Calculate average employment rate from non-zero values
        rates = [float(d.employment_rate or 0) for d in mgnrega_data if (d.employment_rate or 0) > 0]
        avg_employment_rate = sum(rates) / len(rates) if rates else 0
        
        # Check if latest data has ANY meaningful values
        has_meaningful_data = (
            (latest.total_workers or 0) > 0 or 
            float(latest.total_expenditure or 0) > 0 or 
            float(latest.total_work_days or 0) > 0 or
            (latest.total_job_cards_issued or 0) > 0 or
            (latest.works_completed or 0) > 0
        )
        
        context = {
            'district': district,
            'latest_data': latest,
            'historical_data': mgnrega_data[:12],
            'total_expenditure': total_expenditure,
            'avg_employment_rate': avg_employment_rate,
            'total_workers': total_workers,
            'total_work_days': total_work_days,
            'data_source': data_source,
            'has_meaningful_data': has_meaningful_data,
            'data_records_count': mgnrega_data.count(),
        }
    else:
        # No data at all
        context = {
            'district': district,
            'latest_data': None,
            'historical_data': [],
            'total_expenditure': 0,
            'avg_employment_rate': 0,
            'total_workers': 0,
            'total_work_days': 0,
            'data_source': 'No data available',
            'has_meaningful_data': False,
            'data_records_count': 0,
        }
    
    return render(request, 'districts/district_detail.html', context)
