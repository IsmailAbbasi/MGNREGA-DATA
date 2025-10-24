from django.db import models

class District(models.Model):
    name = models.CharField(max_length=100)
    state = models.CharField(max_length=100)

    def __str__(self):
        return self.name

class PerformanceMetric(models.Model):
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    year = models.IntegerField()
    total_work_days = models.IntegerField()
    completed_work_days = models.IntegerField()
    expenditure = models.DecimalField(max_digits=10, decimal_places=2)

    def __str__(self):
        return f"{self.district.name} - {self.year}"