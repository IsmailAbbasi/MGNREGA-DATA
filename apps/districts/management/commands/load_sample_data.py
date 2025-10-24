from django.core.management.base import BaseCommand
from apps.districts.models import District, MGNREGAData
import random

class Command(BaseCommand):
    help = 'Load sample MGNREGA data for Maharashtra districts'

    def handle(self, *args, **kwargs):
        # Maharashtra districts with coordinates
        districts_data = [
            {'name': 'Mumbai', 'code': 'MH-MUM', 'pop': 12442373, 'lat': 19.0760, 'lon': 72.8777},
            {'name': 'Pune', 'code': 'MH-PUN', 'pop': 9429408, 'lat': 18.5204, 'lon': 73.8567},
            {'name': 'Nagpur', 'code': 'MH-NAG', 'pop': 4653570, 'lat': 21.1458, 'lon': 79.0882},
            {'name': 'Thane', 'code': 'MH-THA', 'pop': 11060148, 'lat': 19.2183, 'lon': 72.9781},
            {'name': 'Nashik', 'code': 'MH-NAS', 'pop': 6109052, 'lat': 19.9975, 'lon': 73.7898},
            {'name': 'Aurangabad', 'code': 'MH-AUR', 'pop': 3701282, 'lat': 19.8762, 'lon': 75.3433},
            {'name': 'Solapur', 'code': 'MH-SOL', 'pop': 4317756, 'lat': 17.6599, 'lon': 75.9064},
            {'name': 'Amravati', 'code': 'MH-AMR', 'pop': 2888445, 'lat': 20.9374, 'lon': 77.7796},
            {'name': 'Kolhapur', 'code': 'MH-KOL', 'pop': 3876001, 'lat': 16.7050, 'lon': 74.2433},
            {'name': 'Ahmednagar', 'code': 'MH-AHM', 'pop': 4543083, 'lat': 19.0948, 'lon': 74.7480},
        ]

        for dist_data in districts_data:
            district, created = District.objects.get_or_create(
                district_code=dist_data['code'],
                defaults={
                    'name': dist_data['name'],
                    'state': 'Maharashtra',
                    'population': dist_data['pop'],
                    'latitude': dist_data['lat'],
                    'longitude': dist_data['lon'],
                }
            )
            
            if created:
                self.stdout.write(self.style.SUCCESS(f'Created district: {district.name}'))
                
                # Create sample MGNREGA data for last 3 years
                for year in [2023, 2024, 2025]:
                    for month in range(1, 13):
                        MGNREGAData.objects.create(
                            district=district,
                            year=year,
                            month=month,
                            total_job_cards_issued=random.randint(5000, 50000),
                            total_workers=random.randint(3000, 40000),
                            total_active_workers=random.randint(2000, 35000),
                            total_work_days=random.uniform(50000, 500000),
                            average_days_per_household=random.uniform(40, 100),
                            total_expenditure=random.uniform(10000000, 100000000),
                            wage_expenditure=random.uniform(7000000, 70000000),
                            material_expenditure=random.uniform(3000000, 30000000),
                            employment_rate=random.uniform(60, 95),
                            works_completed=random.randint(100, 1000),
                            works_in_progress=random.randint(50, 500),
                        )
                
                self.stdout.write(self.style.SUCCESS(f'Created MGNREGA data for {district.name}'))

        self.stdout.write(self.style.SUCCESS('Successfully loaded sample data'))