from django.core.management.base import BaseCommand
from apps.districts.models import District, MGNREGAData
from django.db.models import Count

class Command(BaseCommand):
    help = 'Check data health and coverage'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('\n=== MGNREGA Data Health Check ===\n'))
        
        # Total counts
        total_districts = District.objects.count()
        total_records = MGNREGAData.objects.count()
        
        self.stdout.write(f"Total Districts: {total_districts}")
        self.stdout.write(f"Total MGNREGA Records: {total_records}")
        
        # Districts with/without data
        districts_with_data = District.objects.annotate(
            data_count=Count('mgnregadata')
        ).filter(data_count__gt=0)
        
        districts_without_data = District.objects.annotate(
            data_count=Count('mgnregadata')
        ).filter(data_count=0)
        
        self.stdout.write(f"\nDistricts WITH data: {districts_with_data.count()}")
        self.stdout.write(f"Districts WITHOUT data: {districts_without_data.count()}")
        
        # Coverage by state
        self.stdout.write(self.style.WARNING('\n=== Coverage by State ==='))
        
        from django.db.models import Q
        states = District.objects.values('state').annotate(
            total=Count('id'),
            with_data=Count('id', filter=Q(mgnregadata__isnull=False))
        ).order_by('-total')
        
        for state in states[:10]:
            pct = (state['with_data'] / state['total'] * 100) if state['total'] > 0 else 0
            self.stdout.write(f"  {state['state']}: {state['with_data']}/{state['total']} ({pct:.1f}%)")
        
        # Sample districts without data
        if districts_without_data.exists():
            self.stdout.write(self.style.WARNING('\n=== Sample Districts Without Data ==='))
            for d in districts_without_data[:10]:
                self.stdout.write(f"  - {d.name}, {d.state} (Code: {d.district_code})")
        
        # Orphaned data check
        orphaned = MGNREGAData.objects.filter(district__isnull=True).count()
        if orphaned > 0:
            self.stdout.write(self.style.ERROR(f'\n⚠️  WARNING: {orphaned} orphaned MGNREGA records found!'))
            self.stdout.write('Run: python manage.py shell -c "from apps.districts.models import MGNREGAData; MGNREGAData.objects.filter(district__isnull=True).delete()"')
        
        self.stdout.write(self.style.SUCCESS('\n✓ Health check complete!'))