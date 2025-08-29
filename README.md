# Teller Banking Integration

Pull all your bank transactions into MySQL for analysis and budgeting.

## 🚀 Quick Start

### 1. Get Teller Credentials
- Sign up at [teller.io](https://teller.io)
- Get your access token and certificates from the dashboard

### 2. Setup
```bash
# Install dependencies
poetry install

# Configure environment
cp .env.example .env
# Edit .env with your credentials
```

### 3. Add Your Certificates
```bash
mkdir certs
# Put certificate.pem and private_key.pem in certs/
```

### 4. Run
```bash
# Start MySQL and sync transactions
./scripts/run.sh

# Or manually:
docker-compose up -d
poetry run python scripts/sync.py
```

## 📊 What You Get

- **All transactions** automatically synced from all accounts
- **MySQL database** with clean, normalized financial data  
- **Incremental updates** - only pulls new transactions
- **Complete transaction history** via automatic pagination

## 🗂️ Project Structure

```
├── teller_integration/     # Core Python package
│   ├── client.py          # Teller API client with pagination
│   ├── models.py          # Pydantic data models
│   └── config.py          # Configuration handling
├── scripts/
│   ├── sync.py           # Incremental sync script
│   └── run.sh            # Quick start script
├── examples/
│   └── simple_fetch.py   # Basic usage example
├── docker-compose.yml    # MySQL setup
└── .env.example         # Configuration template
```

## 🔧 Configuration

**Required:**
- `TELLER_ACCESS_TOKEN` - Your Teller access token
- `TELLER_CERT_FILE` - Path to certificate.pem
- `TELLER_KEY_FILE` - Path to private_key.pem

**Optional:**
- `DATABASE_URL` - MySQL connection (defaults to local)

## 💾 Database Schema

```sql
institutions    # Banks (Wells Fargo, etc.)
accounts        # Your accounts (checking, savings, credit)
transactions    # All transactions with categories
sync_runs       # Sync history and stats
```

**Key features:**
- Proper financial precision (`DECIMAL(15,2)`)
- Optimized indexes for date/account queries
- Audit trail of all sync operations

## 🔄 Usage

```bash
# Regular sync (incremental)
./scripts/run.sh

# Force full resync
./scripts/run.sh --full

# Export to CSV for analysis
./scripts/export.sh

# Generate daily balance history (time series)
./scripts/balance_history.sh

# Generate complete portfolio with S&P 500 simulation
./scripts/portfolio.sh

# Check sync history
mysql -h localhost -u teller_user -p teller_db
SELECT * FROM sync_runs ORDER BY started_at DESC;
```

## 📊 Analyze Your Data

### CSV Export (Recommended)
```bash
# Export everything to CSV files
./scripts/export.sh

# Creates: exports/teller_export_TIMESTAMP/
#   ├── transactions.csv        # All 922+ transactions  
#   ├── accounts.csv           # Account details
#   ├── monthly_spending.csv   # Spending by month/category
#   ├── recent_transactions.csv # Latest transactions with account names
#   └── README.txt             # Summary stats

# Generate daily balance history (time series)
./scripts/balance_history.sh

# Creates: exports/daily_balances_TIMESTAMP.csv
#   - One row per account per day (4,404+ records)
#   - Shows balance reconstruction going back 2+ years
#   - Perfect for creating balance trend charts in Excel
```

### Direct SQL Queries
```sql
-- Monthly spending by category
SELECT YEAR(date) as year, MONTH(date) as month, 
       category, SUM(ABS(amount)) as total
FROM transactions 
WHERE amount < 0 
GROUP BY year, month, category;

-- Account balances
SELECT a.name, a.balance_amount, i.name as bank
FROM accounts a 
JOIN institutions i ON a.institution_id = i.id;

-- Recent transactions  
SELECT date, description, amount, category
FROM transactions 
ORDER BY date DESC 
LIMIT 50;
```

## 🎯 Features

✅ **Complete data** - Pagination gets ALL available transactions  
✅ **Incremental sync** - Only fetches new data after first run  
✅ **Rate limit handling** - Graceful API error management  
✅ **Data validation** - Pydantic models ensure clean data  
✅ **MySQL optimization** - Proper indexes and foreign keys  
✅ **Docker setup** - One-command MySQL deployment  

## 🔒 Security

- Certificates stored in `certs/` (git ignored)
- Environment variables for all secrets
- No sensitive data in code or logs
- MySQL with authentication required

---

**Ready to analyze your finances? Run `./scripts/run.sh` and start exploring your data!** 🎉
