import requests
import logging
from django.core.cache import cache
from django.conf import settings
from apps.districts.models import District, MGNREGAData
from datetime import datetime
from decimal import Decimal, InvalidOperation
from django.db.models import Count
import calendar
import re

logger = logging.getLogger(__name__)

class MGNREGADataService:
    """Service to fetch and cache MGNREGA data from government API"""
    
    API_BASE_URL = settings.MGNREGA_API_BASE_URL
    API_KEY = settings.MGNREGA_API_KEY
    RESOURCE_IDS = settings.MGNREGA_RESOURCE_IDS
    
    CACHE_TIMEOUT = 3600 * 24  # 24 hours
    
    @staticmethod
    def parse_month(month_str):
        """Parse month string to month number"""
        if not month_str or month_str == 'NA':
            return None
        
        month_str = str(month_str).strip().lower()
        
        month_map = {
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
            'dec': 12, 'december': 12
        }
        
        return month_map.get(month_str)
    
    @staticmethod
    def parse_year(year_str):
        """Parse financial year string to year"""
        if not year_str:
            return None
        
        year_str = str(year_str).strip()
        
        if '-' in year_str:
            parts = year_str.split('-')
            return int(parts[0])
        
        return int(year_str)
    
    @staticmethod
    def safe_int(value, default=0):
        """Safely convert value to integer"""
        if value is None or value == '' or value == 'NA':
            return default
        try:
            return int(float(str(value)))
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def safe_decimal(value, default='0.00'):
        """Safely convert value to Decimal"""
        if value is None or value == '' or value == 'NA':
            return Decimal(default)
        try:
            return Decimal(str(value))
        except (ValueError, TypeError, InvalidOperation):
            return Decimal(default)
    
    @staticmethod
    def fetch_all_states_data(force_refresh=False):
        """Fetch data for all states from API with caching"""
        cache_key = 'mgnrega_all_states_data'
        
        if not force_refresh:
            cached_data = cache.get(cache_key)
            if cached_data:
                logger.info("Returning cached data for all states")
                return cached_data
        
        try:
            resource_id = MGNREGADataService.RESOURCE_IDS.get('district_performance')
            
            params = {
                'api-key': MGNREGADataService.API_KEY,
                'format': 'json',
                'limit': 10000,
            }
            
            url = f"{MGNREGADataService.API_BASE_URL}/{resource_id}"
            
            logger.info(f"Fetching all states data from API: {url}")
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if data and 'records' in data:
                cache.set(cache_key, data, MGNREGADataService.CACHE_TIMEOUT)
                logger.info(f"Fetched and cached {len(data['records'])} records")
                return data
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching all states data: {e}")
            return None
    
    @staticmethod
    def sync_data_to_database(district_code, api_response):
        """Sync API data to database with CORRECT field mapping"""
        if not api_response or not api_response.get('success'):
            return False
        
        records = api_response.get('records', [])
        if not records:
            logger.warning(f"No records to sync for district {district_code}")
            return False
        
        try:
            district = District.objects.get(district_code=district_code)
        except District.DoesNotExist:
            logger.error(f"District not found: {district_code}")
            return False
        
        synced_count = 0
        
        for record in records:
            try:
                # Parse year and month
                year = MGNREGADataService.parse_year(record.get('fin_year'))
                month = MGNREGADataService.parse_month(record.get('month'))
                
                if not year:
                    logger.warning(f"Invalid year in record: {record.get('fin_year')}")
                    continue
                
                # Map API fields to model fields correctly
                data_dict = {
                    'district': district,
                    'year': year,
                    'month': month,
                    
                    # Workers - API field: Total_Individuals_Worked
                    'total_workers': MGNREGADataService.safe_int(record.get('Total_Individuals_Worked')),
                    'total_active_workers': MGNREGADataService.safe_int(record.get('Total_No_of_Active_Workers')),
                    
                    # Job Cards - API field: Total_No_of_JobCards_issued
                    'total_job_cards_issued': MGNREGADataService.safe_int(record.get('Total_No_of_JobCards_issued')),
                    
                    # Work Days - API field: Persondays_of_Central_Liability_so_far
                    'total_work_days': MGNREGADataService.safe_decimal(record.get('Persondays_of_Central_Liability_so_far', 0)),
                    
                    # Average days - API field: Average_days_of_employment_provided_per_Household
                    'average_days_per_household': MGNREGADataService.safe_decimal(
                        record.get('Average_days_of_employment_provided_per_Household', 0)
                    ),
                    
                    # Expenditure - API fields: Total_Exp, Wages, Material_and_skilled_Wages
                    'total_expenditure': MGNREGADataService.safe_decimal(record.get('Total_Exp', 0)),
                    'wage_expenditure': MGNREGADataService.safe_decimal(record.get('Wages', 0)),
                    'material_expenditure': MGNREGADataService.safe_decimal(record.get('Material_and_skilled_Wages', 0)),
                    
                    # Works - API fields: Number_of_Completed_Works, Number_of_Ongoing_Works
                    'works_completed': MGNREGADataService.safe_int(record.get('Number_of_Completed_Works')),
                    'works_in_progress': MGNREGADataService.safe_int(record.get('Number_of_Ongoing_Works')),
                    
                    # Calculate employment rate
                    'employment_rate': MGNREGADataService.safe_decimal(
                        record.get('Average_days_of_employment_provided_per_Household', 0)
                    ) / Decimal('100') * Decimal('100') if record.get('Average_days_of_employment_provided_per_Household') else Decimal('0'),
                }
                
                # Update or create record
                mgnrega_record, created = MGNREGAData.objects.update_or_create(
                    district=district,
                    year=year,
                    month=month,
                    defaults=data_dict
                )
                
                synced_count += 1
                
                if created:
                    logger.debug(f"Created new record for {district.name} - {year}/{month}")
                else:
                    logger.debug(f"Updated record for {district.name} - {year}/{month}")
                
            except Exception as e:
                logger.error(f"Error syncing record: {e}")
                logger.error(f"Record data: {record}")
                continue
        
        logger.info(f"Synced {synced_count} records for district {district_code}")
        return synced_count > 0
    
    @staticmethod
    def fetch_district_data(district_code, force_refresh=False):
        """Fetch data for a specific district"""
        cache_key = f'mgnrega_district_{district_code}'
        
        if not force_refresh:
            cached_data = cache.get(cache_key)
            if cached_data:
                logger.info(f"Returning cached data for district {district_code}")
                return cached_data
        
        try:
            resource_id = MGNREGADataService.RESOURCE_IDS.get('district_performance')
            
            params = {
                'api-key': MGNREGADataService.API_KEY,
                'format': 'json',
                'limit': 1000,
                'filters': f'{{"district_code": "{district_code}"}}'
            }
            
            url = f"{MGNREGADataService.API_BASE_URL}/{resource_id}"
            
            logger.info(f"Fetching district data: {url} with params: {params}")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data and 'records' in data and len(data['records']) > 0:
                    result = {
                        'success': True,
                        'records': data['records'],
                        'source': 'api'
                    }
                    
                    # Sync to database
                    MGNREGADataService.sync_data_to_database(district_code, result)
                    
                    # Cache the result
                    cache.set(cache_key, result, MGNREGADataService.CACHE_TIMEOUT)
                    
                    logger.info(f"API fetch successful for district {district_code}: {len(data['records'])} records")
                    return result
            
            # If API fails, try to return from database
            logger.warning(f"API fetch failed for district {district_code}, trying database fallback")
            return MGNREGADataService._get_database_fallback(district_code)
            
        except Exception as e:
            logger.error(f"API fetch failed for district {district_code}: {e}")
            return MGNREGADataService._get_database_fallback(district_code)
    
    @staticmethod
    def _get_database_fallback(district_code):
        """Get data from database as fallback"""
        try:
            district = District.objects.get(district_code=district_code)
            records = MGNREGAData.objects.filter(district=district).values()
            
            if records:
                logger.info(f"Returning database fallback for district {district_code}")
                return {
                    'success': True,
                    'records': list(records),
                    'source': 'database'
                }
        except Exception as e:
            logger.error(f"Database fallback failed: {e}")
        
        return {'success': False, 'records': [], 'source': 'none'}
    
    @staticmethod
    def bulk_sync_all_states(force_refresh=False, skip_existing=False):
        """Sync data for all districts across all states"""
        logger.info(f"Starting bulk sync for ALL states (skip_existing={skip_existing})")
        
        all_data = MGNREGADataService.fetch_all_states_data(force_refresh)
        
        if not all_data or 'records' not in all_data:
            logger.error("Failed to fetch data from API")
            return False
        
        logger.info(f"Fetched {len(all_data['records'])} total records from API")
        
        all_districts = District.objects.all()
        
        if skip_existing:
            all_districts = all_districts.annotate(
                data_count=Count('mgnregadata')
            ).filter(data_count=0)
            logger.info(f"Skipping districts with existing data. Processing {all_districts.count()} districts")
        
        total_districts = all_districts.count()
        synced_count = 0
        skipped_count = 0
        
        states = all_districts.values_list('state', flat=True).distinct()
        
        for state in states:
            logger.info(f"Processing {state}...")
            state_districts = all_districts.filter(state=state)
            state_synced = 0
            
            for district in state_districts:
                district_records = [
                    r for r in all_data['records']
                    if (
                        r.get('district_code') == district.district_code or
                        (r.get('district_name', '').lower().strip() == district.name.lower().strip() and 
                         r.get('state_name', '').lower().strip() == district.state.lower().strip())
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
                else:
                    skipped_count += 1
                    logger.warning(f"  ✗ No API data found for {district.name}")
            
            logger.info(f"Completed {state}: {state_synced}/{state_districts.count()} districts synced")
        
        logger.info(f"Bulk sync complete: {synced_count} synced, {skipped_count} skipped out of {total_districts} districts")
        return True
    
    @staticmethod
    def bulk_sync_all_districts(state='Maharashtra', force_refresh=False, skip_existing=False):
        """Sync data for all districts in a specific state"""
        logger.info(f"Starting bulk sync for {state} (skip_existing={skip_existing})")
        
        all_data = MGNREGADataService.fetch_all_states_data(force_refresh)
        
        if not all_data or 'records' not in all_data:
            logger.error("Failed to fetch data from API")
            return False
        
        districts = District.objects.filter(state=state)
        
        if skip_existing:
            districts = districts.annotate(
                data_count=Count('mgnregadata')
            ).filter(data_count=0)
            logger.info(f"Processing {districts.count()} districts without data")
        
        synced_count = 0
        
        for district in districts:
            district_records = [
                r for r in all_data['records']
                if r.get('district_code') == district.district_code or
                   (r.get('district_name', '').lower().strip() == district.name.lower().strip())
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