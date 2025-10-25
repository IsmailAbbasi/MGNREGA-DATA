from django.core.management.base import BaseCommand
from apps.performance.services import MGNREGADataService
from apps.districts.models import District, MGNREGAData
from django.conf import settings
from django.db.models import Count
import requests
import time
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Sync MGNREGA data from Government API to database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--state',
            type=str,
            default=None,
            help='Specific state to sync (optional, syncs all states if not provided)'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force refresh cached data'
        )
        parser.add_argument(
            '--bulk',
            action='store_true',
            help='Use bulk sync (faster, single API call)'
        )
        parser.add_argument(
            '--skip-existing',
            action='store_true',
            help='Skip districts that already have data (much faster)'
        )
        parser.add_argument(
            '--direct-api',
            action='store_true',
            help='Fetch directly from API instead of using service layer'
        )

    def handle(self, *args, **options):
        state_filter = options['state']
        force_refresh = options['force']
        use_bulk = options['bulk']
        skip_existing = options['skip_existing']
        direct_api = options['direct_api']
        
        if state_filter:
            self.stdout.write(self.style.SUCCESS(f'Starting data sync for {state_filter}...'))
        else:
            self.stdout.write(self.style.SUCCESS('Starting data sync for ALL STATES across India...'))
        
        # Direct API fetch (bypasses service layer caching issues)
        if direct_api or use_bulk:
            self.stdout.write('Using direct API fetch mode...')
            self._direct_api_sync(state_filter, skip_existing)
            return
        
        # Original service-based sync
        if use_bulk:
            self.stdout.write('Using bulk sync mode...')
            
            if state_filter:
                success = MGNREGADataService.bulk_sync_all_districts(state_filter, force_refresh, skip_existing)
            else:
                success = MGNREGADataService.bulk_sync_all_states(force_refresh, skip_existing)
            
            if success:
                self.stdout.write(self.style.SUCCESS('✓ Bulk sync completed successfully'))
            else:
                self.stdout.write(self.style.ERROR('✗ Bulk sync failed'))
        else:
            # Individual sync
            if state_filter:
                districts = District.objects.filter(state=state_filter)
            else:
                districts = District.objects.all()
            
            if skip_existing:
                districts = districts.annotate(
                    data_count=Count('mgnregadata')
                ).filter(data_count=0)
                self.stdout.write(f'Syncing only {districts.count()} districts without data')
            
            total = districts.count()
            success_count = 0
            fail_count = 0
            
            states = districts.values_list('state', flat=True).distinct()
            
            for state in states:
                self.stdout.write(self.style.WARNING(f'\n=== Processing {state} ==='))
                state_districts = districts.filter(state=state)
                
                for idx, district in enumerate(state_districts, 1):
                    self.stdout.write(f'[{idx}/{state_districts.count()}] Syncing {district.name}...')
                    
                    try:
                        data = MGNREGADataService.fetch_district_data(
                            district.district_code,
                            force_refresh=force_refresh
                        )
                        
                        if data and data.get('success'):
                            success_count += 1
                            source = data.get('source', 'unknown')
                            records = len(data.get('records', []))
                            self.stdout.write(
                                self.style.SUCCESS(f'  ✓ Synced {records} records from {source}')
                            )
                        else:
                            fail_count += 1
                            self.stdout.write(self.style.WARNING(f'  ✗ No data available'))
                        
                        time.sleep(0.5)
                        
                    except Exception as e:
                        fail_count += 1
                        self.stdout.write(self.style.ERROR(f'  ✗ Error: {str(e)}'))
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'\n✓ Sync complete: {success_count} succeeded, {fail_count} failed out of {total} total districts'
                )
            )

    def _direct_api_sync(self, state_filter, skip_existing):
        """Direct API sync - fetches all data in one go"""
        try:
            resource_id = settings.MGNREGA_RESOURCE_IDS.get('district_performance')
            api_key = settings.MGNREGA_API_KEY
            base_url = settings.MGNREGA_API_BASE_URL
            
            if not api_key or api_key == 'your-api-key-here':
                self.stdout.write(self.style.ERROR('❌ MGNREGA_API_KEY not set!'))
                return
            
            self.stdout.write(f'API: {base_url}/{resource_id}')
            self.stdout.write(f'Key: {api_key[:10]}...\n')
            
            # Get districts to process
            if state_filter:
                districts = District.objects.filter(state=state_filter)
            else:
                districts = District.objects.all()
            
            if skip_existing:
                districts = districts.annotate(
                    data_count=Count('mgnregadata')
                ).filter(data_count=0)
            
            district_count = districts.count()
            self.stdout.write(f'Will process {district_count} districts\n')
            
            if district_count == 0:
                self.stdout.write(self.style.WARNING('No districts to process!'))
                return
            
            # Fetch ALL data from API
            self.stdout.write('Fetching ALL data from API...')
            all_records = []
            offset = 0
            batch_size = 1000
            max_batches = 100
            
            for batch_num in range(max_batches):
                params = {
                    'api-key': api_key,
                    'format': 'json',
                    'limit': batch_size,
                    'offset': offset,
                }
                
                if state_filter:
                    import json
                    params['filters'] = json.dumps({'state_name': state_filter.upper()})
                
                try:
                    response = requests.get(
                        f"{base_url}/{resource_id}",
                        params=params,
                        timeout=60
                    )
                    response.raise_for_status()
                    
                    data = response.json()
                    records = data.get('records', [])
                    
                    if not records:
                        self.stdout.write(f'  No more records at offset {offset}')
                        break
                    
                    all_records.extend(records)
                    self.stdout.write(f'  Batch {batch_num + 1}: Fetched {len(records)} records (total: {len(all_records)})')
                    
                    if len(records) < batch_size:
                        self.stdout.write(f'  Reached end of data (got {len(records)} < {batch_size})')
                        break
                    
                    offset += batch_size
                    time.sleep(0.5)
                    
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f'Error at offset {offset}: {str(e)}'))
                    if all_records:
                        break
                    else:
                        raise
            
            self.stdout.write(self.style.SUCCESS(f'\n✓ Fetched {len(all_records)} total records from API\n'))
            
            if not all_records:
                self.stdout.write(self.style.ERROR('❌ No records fetched from API!'))
                return
            
            # Match and save records
            self.stdout.write('Matching records with districts...\n')
            
            # Create district map
            district_map = {}
            for district in districts:
                code = str(district.district_code)
                district_map[code] = district
                district_map[code.upper()] = district
                district_map[code.lower()] = district
                district_map[code.zfill(4)] = district
            
            self.stdout.write(f'District map created with {len(districts)} districts\n')
            
            created_count = 0
            updated_count = 0
            unmatched_count = 0
            error_count = 0
            
            # Show first record
            if all_records:
                first_record = all_records[0]
                self.stdout.write('Sample API record fields:')
                self.stdout.write(f'  district_code: {first_record.get("district_code")}')
                self.stdout.write(f'  district_name: {first_record.get("district_name")}')
                self.stdout.write(f'  state_name: {first_record.get("state_name")}')
                self.stdout.write(f'  fin_year: {first_record.get("fin_year")}')
                self.stdout.write(f'  month: {first_record.get("month")}\n')
            
            # Month name to number mapping
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
                'dec': 12, 'december': 12,
            }
            
            for idx, record in enumerate(all_records, 1):
                try:
                    # Extract district code
                    district_code = (
                        record.get('district_code') or
                        record.get('dist_code') or
                        record.get('districtcode') or
                        record.get('District_Code')
                    )
                    
                    if not district_code:
                        unmatched_count += 1
                        continue
                    
                    # Find district
                    district = None
                    for code_variation in [str(district_code), str(district_code).upper(), 
                                         str(district_code).lower(), str(district_code).zfill(4)]:
                        district = district_map.get(code_variation)
                        if district:
                            break
                    
                    if not district:
                        unmatched_count += 1
                        if unmatched_count <= 5:
                            self.stdout.write(self.style.WARNING(
                                f'  Unmatched: {record.get("district_name")} (code: {district_code})'
                            ))
                        continue
                    
                    # Extract year and month
                    year_str = record.get('fin_year', record.get('financial_year', '2024-2025'))
                    month_str = record.get('month', record.get('Month', 'December'))
                    
                    # Convert financial year to start year (2024-2025 -> 2024)
                    try:
                        if '-' in str(year_str):
                            year_num = int(str(year_str).split('-')[0])
                        else:
                            year_num = int(year_str)
                    except (ValueError, AttributeError):
                        year_num = 2024
                    
                    # Convert month name to number
                    month_num = month_map.get(str(month_str).lower(), 12)
                    
                    # Map API fields to ACTUAL model fields
                    total_job_cards = self._parse_int(record.get('Total_No_of_JobCards_issued', 0))
                    total_workers = self._parse_int(record.get('Total_No_of_Workers', 0))
                    total_active_workers = self._parse_int(record.get('Total_No_of_Active_Workers', 0))
                    total_work_days = self._parse_decimal(record.get('Persondays_of_Central_Liability_so_far', 0))
                    avg_days = self._parse_decimal(record.get('Average_days_of_employment_provided_per_Household', 0))
                    total_exp = self._parse_decimal(record.get('Total_Exp', 0))
                    wage_exp = self._parse_decimal(record.get('Wages', 0))
                    material_exp = self._parse_decimal(record.get('Material_and_skilled_Wages', 0))
                    works_completed = self._parse_int(record.get('Number_of_Completed_Works', 0))
                    works_ongoing = self._parse_int(record.get('Number_of_Ongoing_Works', 0))
                    
                    # Calculate employment rate
                    total_households = self._parse_int(record.get('Total_Households_Worked', 0))
                    active_job_cards = self._parse_int(record.get('Total_No_of_Active_Job_Cards', 0))
                    employment_rate = (total_households / active_job_cards * 100) if active_job_cards > 0 else 0
                    
                    # Create or update MGNREGA data with ACTUAL model fields
                    mgnrega_data, created = MGNREGAData.objects.update_or_create(
                        district=district,
                        year=year_num,
                        month=month_num,
                        defaults={
                            'total_job_cards_issued': total_job_cards,
                            'total_workers': total_workers,
                            'total_active_workers': total_active_workers,
                            'total_work_days': total_work_days,
                            'average_days_per_household': avg_days,
                            'total_expenditure': total_exp,
                            'wage_expenditure': wage_exp,
                            'material_expenditure': material_exp,
                            'employment_rate': employment_rate,
                            'works_completed': works_completed,
                            'works_in_progress': works_ongoing,
                        }
                    )
                    
                    if created:
                        created_count += 1
                        if created_count <= 10:
                            self.stdout.write(self.style.SUCCESS(
                                f'  ✓ Created: {district.name}, {district.state} - {year_num}/{month_num} '
                                f'(Workers: {total_workers}, Days: {total_work_days})'
                            ))
                        elif created_count == 11:
                            self.stdout.write('  ... (showing first 10 only, continuing...)')
                    else:
                        updated_count += 1
                    
                    if idx % 1000 == 0:
                        self.stdout.write(f'  Processed {idx}/{len(all_records)} records...')
                    
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:
                        logger.warning(f"Error processing record {idx}: {str(e)}")
                        self.stdout.write(self.style.WARNING(f'  Error on record {idx}: {str(e)}'))
            
            # Final summary
            final_count = MGNREGAData.objects.count()
            districts_with_data = District.objects.annotate(
                data_count=Count('mgnregadata')
            ).filter(data_count__gt=0).count()
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'\n{"="*60}\n'
                    f'✓ SYNC COMPLETE!\n'
                    f'{"="*60}\n'
                    f'  API records fetched: {len(all_records)}\n'
                    f'  Records created: {created_count}\n'
                    f'  Records updated: {updated_count}\n'
                    f'  Unmatched records: {unmatched_count}\n'
                    f'  Errors: {error_count}\n'
                    f'  Total MGNREGA records in DB: {final_count}\n'
                    f'  Districts with data: {districts_with_data}/{District.objects.count()}\n'
                    f'{"="*60}'
                )
            )
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Fatal error: {str(e)}'))
            import traceback
            traceback.print_exc()
    
    def _parse_int(self, value):
        try:
            if isinstance(value, (int, float)):
                return int(value)
            if value is None or value == '':
                return 0
            return int(str(value).replace(',', '').replace(' ', ''))
        except (ValueError, TypeError):
            return 0
    
    def _parse_float(self, value):
        try:
            if isinstance(value, (int, float)):
                return float(value)
            if value is None or value == '':
                return 0.0
            return float(str(value).replace(',', '').replace(' ', ''))
        except (ValueError, TypeError):
            return 0.0
    
    def _parse_decimal(self, value):
        """Parse decimal values for DecimalField"""
        try:
            if isinstance(value, (int, float)):
                return float(value)
            if value is None or value == '':
                return 0.0
            return float(str(value).replace(',', '').replace(' ', ''))
        except (ValueError, TypeError):
            return 0.0