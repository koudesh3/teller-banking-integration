#!/bin/bash
# Quick sync script

set -e

# Check if .env exists
if [ ! -f .env ]; then
    echo "❌ .env file not found. Copy .env.example and configure it first."
    exit 1
fi

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "📋 Loading DATABASE_URL from .env..."
    export DATABASE_URL="mysql+pymysql://teller_user:teller_pass@localhost:3306/teller_db"
fi

# Start MySQL if not running
if ! docker ps | grep -q teller_mysql; then
    echo "🚀 Starting MySQL..."
    docker-compose up -d
    echo "⏳ Waiting for MySQL to be ready..."
    sleep 10
fi

# Run sync
echo "🔄 Running Teller sync..."
poetry run python scripts/sync.py $@

echo "✅ Sync complete!"