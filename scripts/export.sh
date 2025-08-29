#!/bin/bash
# Quick CSV export script

set -e

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "📋 Loading DATABASE_URL from .env..."
    export DATABASE_URL="mysql+pymysql://teller_user:teller_pass@localhost:3306/teller_db"
fi

# Export to CSV
echo "📊 Exporting Teller data to CSV..."
poetry run python scripts/export_csv.py $@

echo "✅ CSV export complete!"
echo "💡 Open the CSV files in Excel, Google Sheets, or your favorite data tool"