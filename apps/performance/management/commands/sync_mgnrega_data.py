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

    def handle(self, *args, **options):
        state_filter = options['state']
        force_refresh = options['force']
        use_bulk = options['bulk']
        skip_existing = options['skip_existing']
        
        if state_filter:
            self.stdout.write(self.style.SUCCESS(f'Starting data sync for {state_filter}...'))
        else:
            self.stdout.write(self.style.SUCCESS('Starting data sync for ALL STATES across India...'))
        
        if use_bulk:
            # Bulk sync - single API call for all districts
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
            
            # Skip districts that already have data if requested
            if skip_existing:
                from django.db.models import Count
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
                        
                        time.sleep(0.5)  # Reduced wait time
                        
                    except Exception as e:
                        fail_count += 1
                        self.stdout.write(self.style.ERROR(f'  ✗ Error: {str(e)}'))
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'\n✓ Sync complete: {success_count} succeeded, {fail_count} failed out of {total} total districts'
                )
            )