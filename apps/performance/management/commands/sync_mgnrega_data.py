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
            default=None,  # Changed from 'Maharashtra' to None
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

    def handle(self, *args, **options):
        state_filter = options['state']
        force_refresh = options['force']
        use_bulk = options['bulk']
        
        if state_filter:
            self.stdout.write(self.style.SUCCESS(f'Starting data sync for {state_filter}...'))
        else:
            self.stdout.write(self.style.SUCCESS('Starting data sync for ALL STATES across India...'))
        
        if use_bulk:
            # Bulk sync - single API call for all districts
            self.stdout.write('Using bulk sync mode...')
            
            if state_filter:
                # Sync single state
                success = MGNREGADataService.bulk_sync_all_districts(state_filter, force_refresh)
            else:
                # Sync all states
                success = MGNREGADataService.bulk_sync_all_states(force_refresh)
            
            if success:
                self.stdout.write(self.style.SUCCESS('✓ Bulk sync completed successfully'))
            else:
                self.stdout.write(self.style.ERROR('✗ Bulk sync failed'))
        else:
            # Individual sync - one API call per district
            if state_filter:
                districts = District.objects.filter(state=state_filter)
            else:
                districts = District.objects.all()
            
            total = districts.count()
            success_count = 0
            fail_count = 0
            
            # Group by state for better logging
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
                    f'\n✓ Sync complete: {success_count} succeeded, {fail_count} failed out of {total} total districts'
                )
            )