#!/usr/bin/env bash
# filepath: /Users/ismail/Desktop/Survey_App/mgnrega-dashboard/build.sh

set -o errexit

echo "========================================="
echo "Starting MGNREGA Dashboard Build"
echo "========================================="

echo ""
echo "Step 1: Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✓ Dependencies installed"

echo ""
echo "Step 2: Collecting static files..."
python manage.py collectstatic --no-input --clear
echo "✓ Static files collected"

echo ""
echo "Step 3: Running database migrations..."
python manage.py migrate --noinput
echo "✓ Migrations complete"

echo ""
echo "Step 4: Creating cache table..."
python manage.py createcachetable
echo "✓ Cache table created"

echo ""
echo "Step 5: Fetching all Indian districts..."
python manage.py fetch_all_districts
DISTRICT_COUNT=$(python manage.py shell -c "from apps.districts.models import District; print(District.objects.count())" 2>/dev/null || echo "0")
echo "✓ Districts fetched: $DISTRICT_COUNT"

if [ "$DISTRICT_COUNT" = "0" ]; then
    echo "❌ ERROR: No districts loaded!"
    echo "Please check fetch_all_districts command"
    exit 1
fi

echo ""
echo "Step 6: Syncing MGNREGA data (5-10 minutes)..."
python manage.py fast_sync || python manage.py sync_mgnrega_data --bulk --skip-existing
DATA_COUNT=$(python manage.py shell -c "from apps.performance.models import MGNREGAData; print(MGNREGAData.objects.count())" 2>/dev/null || echo "0")
echo "✓ MGNREGA records synced: $DATA_COUNT"

echo ""
echo "========================================="
echo "Build Complete!"
echo "Districts: $DISTRICT_COUNT"
echo "MGNREGA Records: $DATA_COUNT"
echo "========================================="