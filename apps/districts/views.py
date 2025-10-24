from django.shortcuts import render, get_object_or_404
from .models import District, MGNREGAData
from apps.performance.services import MGNREGADataService
import logging

logger = logging.getLogger(__name__)

def district_list(request):
    districts = District.objects.all().order_by('name')
    return render(request, 'districts/district_list.html', {'districts': districts})

def district_detail(request, district_id):
    district = get_object_or_404(District, id=district_id)
    
    # Debug logging
    logger.info(f"Loading data for: {district.name}, {district.state}")
    logger.info(f"District ID: {district.id}, Code: {district.district_code}")
    
    # Get data from database first (we know we have 100% coverage!)
    mgnrega_data = MGNREGAData.objects.filter(district=district).order_by('-year', '-month')
    
    # Debug: Check if query returns anything
    data_count = mgnrega_data.count()
    logger.info(f"Found {data_count} MGNREGA records for {district.name}")
    
    if data_count > 0:
        logger.info(f"Latest record: Year={mgnrega_data.first().year}, Month={mgnrega_data.first().month}")
    
    # Determine data source
    data_source = 'Database (cached)'
    api_status = 'offline'
    
    # Try to refresh from API in background (optional)
    try:
        api_data = MGNREGADataService.fetch_district_data(district.district_code)
        if api_data and api_data.get('source') == 'api':
            data_source = f"Live from Government API"
            api_status = 'connected'
    except Exception as e:
        logger.warning(f"API check failed: {e}")
    
    # Calculate aggregates
    if mgnrega_data.exists():
        latest = mgnrega_data.first()
        
        # Debug: Print latest data
        logger.info(f"Latest data - Workers: {latest.total_workers}, Expenditure: {latest.total_expenditure}")
        
        total_expenditure = sum(float(d.total_expenditure) for d in mgnrega_data)
        avg_employment_rate = sum(float(d.employment_rate) for d in mgnrega_data) / mgnrega_data.count()
        total_workers = sum(d.total_workers for d in mgnrega_data)
        
        context = {
            'district': district,
            'latest_data': latest,
            'historical_data': mgnrega_data[:12],  # Last 12 months
            'total_expenditure': total_expenditure,
            'avg_employment_rate': avg_employment_rate,
            'total_workers': total_workers,
            'data_source': data_source,
            'api_status': api_status,
        }
    else:
        # This should NEVER happen with 100% coverage, but just in case
        logger.error(f"❌ No data found for {district.name} despite 100% coverage!")
        logger.error(f"District ID: {district.id}, Code: {district.district_code}")
        
        # Try to manually check what's in the database
        all_data_for_district = MGNREGAData.objects.filter(district_id=district.id)
        logger.error(f"Manual check by district_id: {all_data_for_district.count()} records")
        
        context = {
            'district': district,
            'latest_data': None,
            'historical_data': [],
            'total_expenditure': 0,
            'avg_employment_rate': 0,
            'total_workers': 0,
            'data_source': 'No data available',
            'api_status': 'offline',
        }
    
    return render(request, 'districts/district_detail.html', context)
