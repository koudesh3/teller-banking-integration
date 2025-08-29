#!/bin/bash
# Generate complete portfolio with S&P 500 simulation

set -e

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "📋 Loading DATABASE_URL from .env..."
    export DATABASE_URL="mysql+pymysql://teller_user:teller_pass@localhost:3306/teller_db"
fi

# Generate complete portfolio
echo "📊 Generating complete portfolio with S&P 500 simulation..."
poetry run python scripts/portfolio_with_sp500.py $@

echo "✅ Complete portfolio export finished!"
echo "💡 Open the CSV in Excel to see your total financial picture including simulated investments"