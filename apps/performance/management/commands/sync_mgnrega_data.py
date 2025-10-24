from django.core.management.base import BaseCommand
from apps.performance.services import MGNREGADataService
from apps.districts.models import District
import time

class Command(BaseCommand):
    help = 'Sync MGNREGA data from Government API to local database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--state',
            type=str,
            default='Maharashtra',
            help='State name to sync data for'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force refresh cached data'
        )

    def handle(self, *args, **options):
        state = options['state']
        force_refresh = options['force']
        
        self.stdout.write(self.style.SUCCESS(f'Starting data sync for {state}...'))
        
        districts = District.objects.filter(state=state)
        total = districts.count()
        success_count = 0
        fail_count = 0
        
        for idx, district in enumerate(districts, 1):
            self.stdout.write(f'[{idx}/{total}] Syncing {district.name}...')
            
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
                        self.style.SUCCESS(
                            f'  ✓ Synced {records} records from {source}'
                        )
                    )
                else:
                    fail_count += 1
                    self.stdout.write(
                        self.style.WARNING(f'  ✗ No data available')
                    )
                
                # Rate limiting - be nice to the API
                time.sleep(1)
                
            except Exception as e:
                fail_count += 1
                self.stdout.write(
                    self.style.ERROR(f'  ✗ Error: {str(e)}')
                )
        
        self.stdout.write(
            self.style.SUCCESS(
                f'\nSync complete: {success_count} succeeded, {fail_count} failed'
            )
        )