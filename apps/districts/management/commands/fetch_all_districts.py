from django.core.management.base import BaseCommand
from apps.districts.models import District
import requests
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Fetch all districts from MGNREGA API and populate database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--state',
            type=str,
            default=None,
            help='Specific state to fetch (optional, fetches all if not provided)'
        )

    def handle(self, *args, **options):
        state_filter = options.get('state')
        
        self.stdout.write(self.style.SUCCESS('Fetching districts from MGNREGA API...'))
        
        try:
            # Fetch data from API
            resource_id = settings.MGNREGA_RESOURCE_IDS.get('district_performance')
            url = f"{settings.MGNREGA_API_BASE_URL}/{resource_id}"
            
            params = {
                'api-key': settings.MGNREGA_API_KEY,
                'format': 'json',
                'limit': 50000,  # Get all records
            }
            
            if state_filter:
                import json
                params['filters'] = json.dumps({'state_name': state_filter})
            
            self.stdout.write(f'Calling API: {url}')
            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            
            if 'records' not in data:
                self.stdout.write(self.style.ERROR('No records found in API response'))
                return
            
            records = data['records']
            self.stdout.write(f'Found {len(records)} records in API')
            
            # Extract unique districts
            districts_map = {}
            
            for record in records:
                # Extract district information (adjust field names based on actual API)
                district_name = (
                    record.get('district_name') or 
                    record.get('districtname') or 
                    record.get('dist_name')
                )
                
                state_name = (
                    record.get('state_name') or 
                    record.get('statename') or 
                    record.get('state')
                )
                
                district_code = (
                    record.get('district_code') or 
                    record.get('dist_code') or 
                    record.get('districtcode')
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
            
            self.stdout.write(f'Found {len(districts_map)} unique districts')
            
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
                    self.stdout.write(
                        self.style.SUCCESS(f'✓ Created: {district.name}, {district.state}')
                    )
                else:
                    updated_count += 1
                    self.stdout.write(f'  Updated: {district.name}, {district.state}')
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'\n✓ Complete! Created: {created_count}, Updated: {updated_count}'
                )
            )
            
        except requests.RequestException as e:
            self.stdout.write(self.style.ERROR(f'API request failed: {str(e)}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
            import traceback
            traceback.print_exc()