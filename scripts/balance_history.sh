#!/bin/bash
# Generate daily balance history

set -e

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "📋 Loading DATABASE_URL from .env..."
    export DATABASE_URL="mysql+pymysql://teller_user:teller_pass@localhost:3306/teller_db"
fi

# Generate balance history
echo "🔄 Reconstructing daily balance history..."
poetry run python scripts/balance_history.py $@

echo "✅ Balance history export complete!"
echo "💡 Open the CSV in Excel to create charts and analyze trends over time"