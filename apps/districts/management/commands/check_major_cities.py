from django.core.management.base import BaseCommand
from apps.districts.models import District, MGNREGAData

class Command(BaseCommand):
    help = 'Check if major cities exist and show their actual names in database'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('\n=== Checking Major Cities ===\n'))
        
        # Search patterns for major cities
        city_patterns = {
            'Mumbai': ['mumbai', 'bombay'],
            'Delhi': ['delhi'],
            'Bangalore': ['bangalore', 'bengaluru'],
            'Chennai': ['chennai', 'madras'],
            'Kolkata': ['kolkata', 'calcutta'],
            'Hyderabad': ['hyderabad'],
            'Ahmedabad': ['ahmedabad', 'ahmadabad'],
            'Pune': ['pune'],
            'Lucknow': ['lucknow'],
            'Jaipur': ['jaipur'],
        }
        
        for city_name, patterns in city_patterns.items():
            self.stdout.write(f"\n{city_name}:")
            found = False
            
            for pattern in patterns:
                matches = District.objects.filter(name__icontains=pattern)
                if matches.exists():
                    found = True
                    for district in matches:
                        data_count = MGNREGAData.objects.filter(district=district).count()
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"  ✓ {district.name}, {district.state} (ID: {district.id}) - {data_count} records"
                            )
                        )
            
            if not found:
                self.stdout.write(self.style.WARNING(f"  ✗ Not found"))
        
        self.stdout.write(self.style.SUCCESS('\n✓ Check complete!'))