from django.db import models
from django.utils import timezone

class District(models.Model):
    name = models.CharField(max_length=100)
    state = models.CharField(max_length=100, default='Maharashtra')
    district_code = models.CharField(max_length=20, unique=True, default='UNKNOWN')
    population = models.IntegerField(null=True, blank=True)
    latitude = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitude = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return f"{self.name}, {self.state}"

class MGNREGAData(models.Model):
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    year = models.IntegerField()
    month = models.IntegerField(null=True, blank=True)
    
    # Key metrics
    total_job_cards_issued = models.IntegerField(default=0)
    total_workers = models.IntegerField(default=0)
    total_active_workers = models.IntegerField(default=0)
    total_work_days = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    average_days_per_household = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    
    # Financial metrics
    total_expenditure = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    wage_expenditure = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    material_expenditure = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    
    # Performance metrics
    employment_rate = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    works_completed = models.IntegerField(default=0)
    works_in_progress = models.IntegerField(default=0)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-year', '-month']
        unique_together = ['district', 'year', 'month']

    def __str__(self):
        return f"{self.district.name} - {self.year}/{self.month or 'Annual'}"