#!/usr/bin/env bash
# exit on error
set -o errexit

echo "========================================="
echo "MGNREGA Dashboard Build Script"
echo "========================================="

echo ""
echo "Step 1: Installing Python dependencies..."
pip install -r requirements.txt

echo ""
echo "Step 2: Collecting static files..."
python manage.py collectstatic --no-input

echo ""
echo "Step 3: Running database migrations..."
python manage.py migrate

echo ""
echo "Step 4: Creating superuser (if needed)..."
python manage.py shell -c "
from django.contrib.auth import get_user_model;
User = get_user_model();
if not User.objects.filter(username='admin').exists():
    User.objects.create_superuser('admin', 'admin@example.com', 'admin123')
    print('Superuser created')
else:
    print('Superuser already exists')
" || echo "Superuser creation skipped"

echo ""
echo "Step 5: Fetching ALL districts (2-3 minutes)..."
python manage.py fetch_all_districts --limit 20000 || echo "Warning: District fetch had issues"
DISTRICT_COUNT=$(python manage.py shell -c "from apps.districts.models import District; print(District.objects.count())" 2>/dev/null || echo "0")
echo "✓ Districts fetched: $DISTRICT_COUNT"

echo ""
echo "Step 6: Syncing MGNREGA data (10-20 minutes)..."
python manage.py sync_mgnrega_data --bulk || echo "Warning: Data sync had issues"
DATA_COUNT=$(python manage.py shell -c "from apps.performance.models import PerformanceMetric; print(PerformanceMetric.objects.count())" 2>/dev/null || echo "0")
echo "✓ MGNREGA records synced: $DATA_COUNT"

echo ""
echo "========================================="
echo "Build Complete!"
echo "Districts: $DISTRICT_COUNT"
echo "MGNREGA Records: $DATA_COUNT"
echo "========================================="