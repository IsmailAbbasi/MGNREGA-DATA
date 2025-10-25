from django.shortcuts import render, get_object_or_404
from django.db.models import Count, Q
from .models import District, MGNREGAData
from apps.performance.services import MGNREGADataService
import logging

logger = logging.getLogger(__name__)

def district_list(request):
    """Display all districts with data availability indicator"""
    # Get all districts with data count - NO LIMIT
    districts = District.objects.annotate(
        data_count=Count('mgnregadata')
    ).order_by('state', 'name')
    
    # Count districts with data
    districts_with_data = districts.filter(data_count__gt=0)
    districts_with_data_count = districts_with_data.count()
    total_districts = districts.count()
    
    # Log for debugging
    logger.info(f"Total districts: {total_districts}, With data: {districts_with_data_count}")
    
    # Get popular districts (only those with data)
    popular_names = ['Bengaluru', 'Lucknow', 'Pune', 'Jaipur', 'Ahmadabad', 
                     'Nagpur', 'Patna', 'Bhopal', 'Indore', 'Kanpur']
    popular_districts = districts_with_data.filter(name__in=popular_names)[:10]
    
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
    
    # Get data from database ONLY (no API call on page load)
    mgnrega_data = MGNREGAData.objects.filter(district=district).order_by('-year', '-month')
    
    data_source = 'Database (Supabase)'
    
    # Prepare context
    if mgnrega_data.exists():
        latest = mgnrega_data.first()
        
        # Calculate aggregates safely
        total_expenditure = sum(
            float(d.total_expenditure or 0) for d in mgnrega_data
        )
        
        total_workers = sum(
            int(d.total_workers or 0) for d in mgnrega_data
        )
        
        total_work_days = sum(
            float(d.total_work_days or 0) for d in mgnrega_data
        )
        
        # Calculate average employment rate
        employment_rates = [
            float(d.employment_rate or 0) 
            for d in mgnrega_data 
            if d.employment_rate is not None
        ]
        avg_employment_rate = (
            sum(employment_rates) / len(employment_rates) 
            if employment_rates else 0
        )
        
        # Check if data is meaningful (not all zeros)
        has_meaningful_data = (
            (latest.total_workers or 0) > 0 or
            (latest.total_expenditure or 0) > 0 or
            (latest.total_work_days or 0) > 0 or
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
            'data_source': 'No Data',
            'has_meaningful_data': False,
            'data_records_count': 0,
        }
    
    return render(request, 'districts/district_detail.html', context)
