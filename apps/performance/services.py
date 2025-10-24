import requests
import logging
from django.core.cache import cache
from django.conf import settings
from apps.districts.models import District, MGNREGAData
from datetime import datetime

logger = logging.getLogger(__name__)

class MGNREGADataService:
    """Service to fetch and cache MGNREGA data from government API"""
    
    API_BASE_URL = settings.MGNREGA_API_BASE_URL
    API_KEY = settings.MGNREGA_API_KEY
    RESOURCE_IDS = settings.MGNREGA_RESOURCE_IDS
    
    CACHE_TIMEOUT = 3600 * 24  # 24 hours
    
    @staticmethod
    def fetch_district_data(district_code, year=None, force_refresh=False):
        """Fetch data for a district with caching and fallback"""
        cache_key = f"mgnrega_data_{district_code}_{year or 'latest'}"
        
        if not force_refresh:
            cached_data = cache.get(cache_key)
            if cached_data:
                logger.info(f"Returning cached data for district {district_code}")
                return cached_data
        
        try:
            # Try to fetch from Government API
            data = MGNREGADataService._fetch_from_api(district_code, year)
            
            if data:
                # Store in cache
                cache.set(cache_key, data, MGNREGADataService.CACHE_TIMEOUT)
                
                # Sync to database for offline access
                MGNREGADataService.sync_data_to_database(district_code, data)
                
                logger.info(f"Fetched fresh data for district {district_code}")
                return data
            
        except Exception as e:
            logger.error(f"API fetch failed for district {district_code}: {str(e)}")
        
        # Fallback to database
        db_data = MGNREGADataService._get_from_database(district_code)
        if db_data:
            logger.info(f"Returning database fallback for district {district_code}")
            return db_data
        
        return None
    
    @staticmethod
    def _fetch_from_api(district_code, year=None):
        """Fetch from data.gov.in API"""
        try:
            resource_id = MGNREGADataService.RESOURCE_IDS.get('district_performance')
            
            # Build API URL
            url = f"{MGNREGADataService.API_BASE_URL}/{resource_id}"
            
            # Parameters for the API call
            params = {
                'api-key': MGNREGADataService.API_KEY,
                'format': 'json',
                'limit': 1000,
            }
            
            # Add filters if available
            filters = {}
            if district_code:
                filters['district_code'] = district_code
            if year:
                filters['financial_year'] = str(year)
            
            if filters:
                params['filters'] = filters
            
            logger.info(f"Calling API: {url} with params: {params}")
            
            # Make API request
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            api_data = response.json()
            
            # Parse the response
            if 'records' in api_data:
                return {
                    'success': True,
                    'records': api_data['records'],
                    'total': api_data.get('total', len(api_data['records'])),
                    'source': 'api'
                }
            else:
                logger.warning(f"No records found in API response for district {district_code}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error parsing API response: {str(e)}")
            return None
    
    @staticmethod
    def _get_from_database(district_code):
        """Fallback to get data from local database"""
        try:
            district = District.objects.get(district_code=district_code)
            data = MGNREGAData.objects.filter(district=district).order_by('-year', '-month')
            
            if data.exists():
                return {
                    'success': True,
                    'district': district,
                    'records': [
                        {
                            'year': d.year,
                            'month': d.month,
                            'total_job_cards_issued': d.total_job_cards_issued,
                            'total_workers': d.total_workers,
                            'total_active_workers': d.total_active_workers,
                            'total_work_days': float(d.total_work_days),
                            'average_days_per_household': float(d.average_days_per_household),
                            'total_expenditure': float(d.total_expenditure),
                            'wage_expenditure': float(d.wage_expenditure),
                            'material_expenditure': float(d.material_expenditure),
                            'employment_rate': float(d.employment_rate),
                            'works_completed': d.works_completed,
                            'works_in_progress': d.works_in_progress,
                        }
                        for d in data
                    ],
                    'latest': data.first(),
                    'source': 'database'
                }
        except District.DoesNotExist:
            logger.error(f"District {district_code} not found")
        except Exception as e:
            logger.error(f"Database query failed: {str(e)}")
        
        return None
    
    @staticmethod
    def sync_data_to_database(district_code, api_data):
        """Store fetched API data to database for offline access"""
        try:
            district = District.objects.get(district_code=district_code)
            
            for record in api_data.get('records', []):
                # Map API fields to model fields (adjust based on actual API response)
                year = record.get('financial_year') or record.get('year') or datetime.now().year
                month = record.get('month')
                
                MGNREGAData.objects.update_or_create(
                    district=district,
                    year=int(year) if year else datetime.now().year,
                    month=int(month) if month else None,
                    defaults={
                        'total_job_cards_issued': int(record.get('total_job_cards', 0)),
                        'total_workers': int(record.get('total_workers', 0)),
                        'total_active_workers': int(record.get('active_workers', 0)),
                        'total_work_days': float(record.get('person_days_generated', 0)),
                        'average_days_per_household': float(record.get('average_days_per_household', 0)),
                        'total_expenditure': float(record.get('total_expenditure', 0)),
                        'wage_expenditure': float(record.get('wage_expenditure', 0)),
                        'material_expenditure': float(record.get('material_expenditure', 0)),
                        'employment_rate': float(record.get('employment_rate', 0)),
                        'works_completed': int(record.get('works_completed', 0)),
                        'works_in_progress': int(record.get('works_in_progress', 0)),
                    }
                )
            
            logger.info(f"Synced {len(api_data.get('records', []))} records to database for district {district_code}")
            return True
            
        except District.DoesNotExist:
            logger.error(f"District {district_code} not found")
            return False
        except Exception as e:
            logger.error(f"Failed to sync data: {str(e)}")
            return False
    
    @staticmethod
    def fetch_all_districts_data(state='Maharashtra', force_refresh=False):
        """Fetch data for all districts in a state"""
        districts = District.objects.filter(state=state)
        results = {}
        
        for district in districts:
            data = MGNREGADataService.fetch_district_data(
                district.district_code,
                force_refresh=force_refresh
            )
            results[district.district_code] = data
        
        return results