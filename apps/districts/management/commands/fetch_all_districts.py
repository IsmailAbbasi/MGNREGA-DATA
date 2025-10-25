from django.core.management.base import BaseCommand
from apps.districts.models import District
import requests
import logging
from django.conf import settings
import time

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Fetch all districts from MGNREGA API'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--state',
            type=str,
            help='Filter by state name',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=10000,  # Increase limit to ensure we get all districts
            help='Maximum records to fetch (default: 10000)',
        )

    def handle(self, *args, **options):
        state_filter = options.get('state')
        max_limit = options.get('limit', 10000)
        
        self.stdout.write(self.style.SUCCESS('Fetching districts from MGNREGA API...'))
        
        try:
            resource_id = settings.MGNREGA_RESOURCE_IDS.get('district_performance')
            url = f"{settings.MGNREGA_API_BASE_URL}/{resource_id}"
            
            all_records = []
            offset = 0
            batch_size = 1000  # Fetch 1000 at a time
            max_attempts = max_limit // batch_size + 1  # Calculate attempts based on limit
            
            self.stdout.write(f'Max attempts: {max_attempts}, Batch size: {batch_size}')
            
            # Paginate through ALL records
            for attempt in range(max_attempts):
                params = {
                    'api-key': settings.MGNREGA_API_KEY,
                    'format': 'json',
                    'limit': batch_size,
                    'offset': offset,
                }
                
                if state_filter:
                    import json
                    params['filters'] = json.dumps({'state_name': state_filter})
                
                self.stdout.write(f'Fetching batch {attempt + 1}: offset={offset}, limit={batch_size}')
                
                try:
                    response = requests.get(url, params=params, timeout=60)
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    if 'records' not in data:
                        self.stdout.write(self.style.WARNING('No records key in response'))
                        break
                    
                    records = data['records']
                    
                    if len(records) == 0:
                        self.stdout.write(f'No more records at offset {offset}')
                        break
                    
                    self.stdout.write(f'  ✓ Got {len(records)} records (total: {len(all_records) + len(records)})')
                    all_records.extend(records)
                    
                    # If we got fewer than batch_size, we're done
                    if len(records) < batch_size:
                        self.stdout.write('Reached end of data')
                        break
                    
                    offset += batch_size
                    time.sleep(0.5)  # Be nice to API
                    
                except requests.exceptions.Timeout:
                    self.stdout.write(self.style.WARNING(f'Timeout at offset {offset}, continuing...'))
                    break
                except requests.RequestException as e:
                    self.stdout.write(self.style.WARNING(f'Error at offset {offset}: {str(e)}'))
                    if offset == 0:
                        raise
                    break
            
            self.stdout.write(self.style.SUCCESS(f'\n✓ Total records fetched: {len(all_records)}'))
            
            if len(all_records) == 0:
                self.stdout.write(self.style.ERROR('❌ No records fetched from API'))
                return
            
            # Extract unique districts
            districts_map = {}
            
            for record in all_records:
                # Try all possible field name variations
                district_name = (
                    record.get('district_name') or 
                    record.get('districtname') or 
                    record.get('dist_name') or
                    record.get('District_Name') or
                    record.get('DISTRICT_NAME')
                )
                
                state_name = (
                    record.get('state_name') or 
                    record.get('statename') or 
                    record.get('state') or
                    record.get('State_Name') or
                    record.get('STATE_NAME')
                )
                
                district_code = (
                    record.get('district_code') or 
                    record.get('dist_code') or 
                    record.get('districtcode') or
                    record.get('District_Code')
                )
                
                if not district_name or not state_name:
                    continue
                
                # Create unique key
                key = f"{state_name}_{district_name}"
                
                if key not in districts_map:
                    districts_map[key] = {
                        'name': district_name.strip().title(),
                        'state': state_name.strip().title(),
                        'district_code': district_code or f"{state_name[:2].upper()}-{district_name[:3].upper()}",
                    }
            
            self.stdout.write(self.style.SUCCESS(f'✓ Found {len(districts_map)} unique districts'))
            
            # Create districts in database
            created_count = 0
            updated_count = 0
            
            for key, dist_data in districts_map.items():
                district, created = District.objects.update_or_create(
                    district_code=dist_data['district_code'],
                    defaults={
                        'name': dist_data['name'],
                        'state': dist_data['state'],
                    }
                )
                
                if created:
                    created_count += 1
                    if created_count <= 20:
                        self.stdout.write(self.style.SUCCESS(f'  ✓ {district.name}, {district.state}'))
                    elif created_count == 21:
                        self.stdout.write('  ... (showing first 20 only)')
                else:
                    updated_count += 1
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'\n{"="*50}\n'
                    f'✓ SUCCESS!\n'
                    f'  Created: {created_count}\n'
                    f'  Updated: {updated_count}\n'
                    f'  Total: {District.objects.count()}\n'
                    f'{"="*50}'
                )
            )
            
        except requests.RequestException as e:
            self.stdout.write(self.style.ERROR(f'❌ API request failed: {str(e)}'))
            raise
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error: {str(e)}'))
            import traceback
            traceback.print_exc()
            raise