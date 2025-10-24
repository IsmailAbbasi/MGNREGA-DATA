import requests
import logging
from django.core.cache import cache
from django.conf import settings
from apps.districts.models import District, MGNREGAData
from datetime import datetime
from decimal import Decimal, InvalidOperation
import calendar
import re

logger = logging.getLogger(__name__)

class MGNREGADataService:
    """Service to fetch and cache MGNREGA data from government API"""
    
    API_BASE_URL = settings.MGNREGA_API_BASE_URL
    API_KEY = settings.MGNREGA_API_KEY
    RESOURCE_IDS = settings.MGNREGA_RESOURCE_IDS
    
    CACHE_TIMEOUT = 3600 * 24  # 24 hours
    
    # Month name to number mapping
    MONTH_MAP = {
        'jan': 1, 'january': 1,
        'feb': 2, 'february': 2,
        'mar': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5,
        'jun': 6, 'june': 6,
        'jul': 7, 'july': 7,
        'aug': 8, 'august': 8,
        'sep': 9, 'september': 9,
        'oct': 10, 'october': 10,
        'nov': 11, 'november': 11,
        'dec': 12, 'december': 12,
    }
    
    @staticmethod
    def parse_month(month_value):
        """Convert month name or number to integer"""
        if not month_value:
            return None
        
        # If already a number
        if isinstance(month_value, int):
            return month_value
        
        # If string number
        if isinstance(month_value, str) and month_value.isdigit():
            return int(month_value)
        
        # If month name
        if isinstance(month_value, str):
            month_lower = month_value.lower().strip()
            return MGNREGADataService.MONTH_MAP.get(month_lower)
        
        return None
    
    @staticmethod
    def parse_year(year_value):
        """Extract year from various formats"""
        if not year_value:
            return datetime.now().year
        
        if isinstance(year_value, int):
            return year_value
        
        # Handle formats like "2024-25", "2024", "FY2024"
        if isinstance(year_value, str):
            # Remove any non-numeric characters except hyphen
            year_str = ''.join(c for c in year_value if c.isdigit() or c == '-')
            
            # If format is "2024-25", take first year
            if '-' in year_str:
                return int(year_str.split('-')[0])
            
            # If just a number
            if year_str.isdigit():
                year = int(year_str)
                # If 2-digit year, convert to 4-digit
                if year < 100:
                    year += 2000
                return year
        
        return datetime.now().year
    
    @staticmethod
    def safe_int(value, default=0):
        """Safely convert value to integer"""
        if value is None or value == '':
            return default
        
        try:
            # Remove commas and convert
            if isinstance(value, str):
                value = value.replace(',', '').strip()
                # Handle "NA", "N/A", "-", etc.
                if value.lower() in ['na', 'n/a', '-', 'nil', 'null']:
                    return default
            return int(float(value))
        except (ValueError, TypeError, AttributeError):
            return default
    
    @staticmethod
    def safe_decimal(value, default=0):
        """Safely convert value to Decimal"""
        if value is None or value == '':
            return Decimal(default)
        
        try:
            # Handle string values
            if isinstance(value, str):
                value = value.strip()
                # Handle "NA", "N/A", "-", etc.
                if value.lower() in ['na', 'n/a', '-', 'nil', 'null', '']:
                    return Decimal(default)
                # Remove commas and any non-numeric characters except dot and hyphen
                value = re.sub(r'[^\d\.\-]', '', value)
                if value == '' or value == '-':
                    return Decimal(default)
            
            return Decimal(str(value))
        except (ValueError, TypeError, AttributeError, InvalidOperation):
            logger.warning(f"Could not convert '{value}' to Decimal, using default {default}")
            return Decimal(default)
    
    @staticmethod
    def fetch_all_states_data(force_refresh=False):
        """Fetch data for all states"""
        cache_key = 'mgnrega_all_states'
        
        if not force_refresh:
            cached = cache.get(cache_key)
            if cached:
                return cached
        
        try:
            resource_id = MGNREGADataService.RESOURCE_IDS.get('district_performance')
            url = f"{MGNREGADataService.API_BASE_URL}/{resource_id}"
            
            params = {
                'api-key': MGNREGADataService.API_KEY,
                'format': 'json',
                'limit': 10000,
            }
            
            logger.info(f"Fetching data from API: {url}")
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if 'records' in data:
                cache.set(cache_key, data, MGNREGADataService.CACHE_TIMEOUT)
                return data
            
            return None
            
        except Exception as e:
            logger.error(f"API fetch failed: {str(e)}")
            return None
    
    @staticmethod
    def fetch_district_data(district_code, year=None, force_refresh=False):
        """Fetch data for a specific district"""
        cache_key = f"mgnrega_data_{district_code}_{year or 'all'}"
        
        if not force_refresh:
            cached_data = cache.get(cache_key)
            if cached_data:
                logger.info(f"Returning cached data for district {district_code}")
                return cached_data
        
        try:
            resource_id = MGNREGADataService.RESOURCE_IDS.get('district_performance')
            url = f"{MGNREGADataService.API_BASE_URL}/{resource_id}"
            
            params = {
                'api-key': MGNREGADataService.API_KEY,
                'format': 'json',
                'limit': 1000,
            }
            
            # Add filters
            filters = {}
            if district_code:
                filters['district_code'] = district_code
            if year:
                filters['financial_year'] = str(year)
            
            if filters:
                import json
                params['filters'] = json.dumps(filters)
            
            logger.info(f"Fetching district data: {url} with params: {params}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            api_data = response.json()
            
            if 'records' in api_data and len(api_data['records']) > 0:
                result = {
                    'success': True,
                    'records': api_data['records'],
                    'total': api_data.get('total', len(api_data['records'])),
                    'source': 'api'
                }
                
                # Cache the result
                cache.set(cache_key, result, MGNREGADataService.CACHE_TIMEOUT)
                
                # Sync to database
                MGNREGADataService.sync_data_to_database(district_code, result)
                
                return result
            else:
                logger.warning(f"No records found for district {district_code}")
                
        except Exception as e:
            logger.error(f"API fetch failed for district {district_code}: {str(e)}")
        
        # Fallback to database
        db_data = MGNREGADataService._get_from_database(district_code)
        if db_data:
            logger.info(f"Returning database fallback for district {district_code}")
            return db_data
        
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
        """Store fetched API data to database"""
        try:
            district = District.objects.get(district_code=district_code)
            synced_count = 0
            
            # Log first record to understand structure
            if api_data.get('records'):
                first_record = api_data['records'][0]
                logger.info(f"Sample record fields: {list(first_record.keys())}")
                logger.info(f"Sample record: {first_record}")
            
            for record in api_data.get('records', []):
                try:
                    # Parse year and month
                    year = MGNREGADataService.parse_year(
                        record.get('financial_year') or 
                        record.get('fin_year') or 
                        record.get('year') or
                        record.get('finyear')
                    )
                    
                    month = MGNREGADataService.parse_month(
                        record.get('month') or 
                        record.get('fin_month') or
                        record.get('month_name')
                    )
                    
                    MGNREGAData.objects.update_or_create(
                        district=district,
                        year=year,
                        month=month,
                        defaults={
                            'total_job_cards_issued': MGNREGADataService.safe_int(
                                record.get('total_job_cards_issued') or 
                                record.get('job_cards_issued') or
                                record.get('total_job_cards')
                            ),
                            'total_workers': MGNREGADataService.safe_int(
                                record.get('total_workers') or 
                                record.get('persons_worked') or
                                record.get('total_persons_worked')
                            ),
                            'total_active_workers': MGNREGADataService.safe_int(
                                record.get('total_active_workers') or 
                                record.get('active_workers') or
                                record.get('active_job_cards')
                            ),
                            'total_work_days': MGNREGADataService.safe_decimal(
                                record.get('persondays_generated') or 
                                record.get('total_persondays') or
                                record.get('person_days_generated')
                            ),
                            'average_days_per_household': MGNREGADataService.safe_decimal(
                                record.get('avg_days_per_household') or 
                                record.get('average_days_employment') or
                                record.get('avg_days_employment')
                            ),
                            'total_expenditure': MGNREGADataService.safe_decimal(
                                record.get('total_exp') or 
                                record.get('total_expenditure') or
                                record.get('total_expenditure_rs')
                            ),
                            'wage_expenditure': MGNREGADataService.safe_decimal(
                                record.get('wage_exp') or 
                                record.get('wages_paid') or
                                record.get('wage_expenditure')
                            ),
                            'material_expenditure': MGNREGADataService.safe_decimal(
                                record.get('material_exp') or 
                                record.get('material_expenditure') or
                                record.get('material_cost')
                            ),
                            'employment_rate': MGNREGADataService.safe_decimal(
                                record.get('employment_rate') or 
                                record.get('employment_percentage') or
                                record.get('employment_percent')
                            ),
                            'works_completed': MGNREGADataService.safe_int(
                                record.get('works_completed') or 
                                record.get('completed_works') or
                                record.get('total_works_completed')
                            ),
                            'works_in_progress': MGNREGADataService.safe_int(
                                record.get('works_ongoing') or 
                                record.get('ongoing_works') or
                                record.get('works_in_progress')
                            ),
                        }
                    )
                    synced_count += 1
                    
                except Exception as e:
                    logger.warning(f"Skipped record due to error: {str(e)}")
                    continue
            
            logger.info(f"Synced {synced_count} records for district {district_code}")
            return synced_count > 0
            
        except District.DoesNotExist:
            logger.error(f"District {district_code} not found")
            return False
        except Exception as e:
            logger.error(f"Failed to sync data: {str(e)}")
            return False
    
    @staticmethod
    def bulk_sync_all_states(force_refresh=False):
        """Sync data for all districts across all states in India"""
        logger.info(f"Starting bulk sync for ALL states")
        
        # First fetch all data from API
        all_data = MGNREGADataService.fetch_all_states_data(force_refresh)
        
        if not all_data or 'records' not in all_data:
            logger.error("Failed to fetch data from API")
            return False
        
        logger.info(f"Fetched {len(all_data['records'])} total records from API")
        
        # Get all districts from database
        all_districts = District.objects.all()
        total_districts = all_districts.count()
        synced_count = 0
        
        # Group by state for logging
        states = all_districts.values_list('state', flat=True).distinct()
        
        for state in states:
            logger.info(f"Processing {state}...")
            state_districts = all_districts.filter(state=state)
            state_synced = 0
            
            for district in state_districts:
                # Filter records for this district (try multiple matching strategies)
                district_records = [
                    r for r in all_data['records']
                    if (
                        r.get('district_code') == district.district_code or
                        r.get('dist_code') == district.district_code or
                        r.get('districtcode') == district.district_code or
                        (r.get('district_name', '').lower() == district.name.lower() and 
                         r.get('state_name', '').lower() == district.state.lower()) or
                        (r.get('districtname', '').lower() == district.name.lower() and 
                         r.get('statename', '').lower() == district.state.lower())
                    )
                ]
                
                if district_records:
                    result = {
                        'success': True,
                        'records': district_records,
                        'source': 'api'
                    }
                    
                    if MGNREGADataService.sync_data_to_database(district.district_code, result):
                        synced_count += 1
                        state_synced += 1
                        logger.info(f"  ✓ Synced {len(district_records)} records for {district.name}")
        
            logger.info(f"Completed {state}: {state_synced}/{state_districts.count()} districts synced")
        
        logger.info(f"Bulk sync complete: {synced_count}/{total_districts} districts synced across all states")
        return True

    # Keep the existing bulk_sync_all_districts for single state
    @staticmethod
    def bulk_sync_all_districts(state='Maharashtra', force_refresh=False):
        """Sync data for all districts in a specific state"""
        logger.info(f"Starting bulk sync for {state}")
        
        # First fetch all data from API
        all_data = MGNREGADataService.fetch_all_states_data(force_refresh)
        
        if not all_data or 'records' not in all_data:
            logger.error("Failed to fetch data from API")
            return False
        
        districts = District.objects.filter(state=state)
        synced_count = 0
        
        for district in districts:
            # Filter records for this district
            district_records = [
                r for r in all_data['records']
                if r.get('district_code') == district.district_code or
                   r.get('dist_code') == district.district_code or
                   r.get('district_name', '').lower() == district.name.lower()
            ]
            
            if district_records:
                result = {
                    'success': True,
                    'records': district_records,
                    'source': 'api'
                }
                
                if MGNREGADataService.sync_data_to_database(district.district_code, result):
                    synced_count += 1
                    logger.info(f"Synced {len(district_records)} records for {district.name}")
        
        logger.info(f"Bulk sync complete: {synced_count}/{districts.count()} districts synced")
        return True