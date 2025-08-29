#!/usr/bin/env python3
"""
Daily Balance History Reconstruction

Reconstructs account balances for each day by replaying transactions backwards.
Creates one row per account per day showing the balance at end of day.
"""

import os
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
from decimal import Decimal

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd


class BalanceHistoryBuilder:
    def __init__(self, mysql_url: str):
        """Initialize balance history builder"""
        self.engine = create_engine(mysql_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    def get_accounts_with_current_balance(self) -> Dict[str, Dict]:
        """Get all accounts with their current balances"""
        result = self.session.execute(text("""
            SELECT 
                a.id,
                a.name,
                a.type,
                a.balance_amount,
                i.name as institution_name
            FROM accounts a
            JOIN institutions i ON a.institution_id = i.id
            WHERE a.status = 'open'
            ORDER BY a.name
        """))
        
        accounts = {}
        for row in result:
            accounts[row.id] = {
                'name': row.name,
                'type': row.type,
                'current_balance': float(row.balance_amount) if row.balance_amount else 0.0,
                'institution': row.institution_name
            }
        
        return accounts
    
    def get_date_range(self) -> tuple:
        """Get the date range of all transactions"""
        result = self.session.execute(text("""
            SELECT MIN(date) as earliest, MAX(date) as latest
            FROM transactions
            WHERE status = 'posted'
        """)).fetchone()
        
        return result.earliest, result.latest
    
    def get_transactions_by_account_and_date(self) -> Dict[str, Dict[str, List]]:
        """Get all transactions organized by account and date"""
        result = self.session.execute(text("""
            SELECT 
                account_id,
                date,
                amount,
                description,
                running_balance,
                id
            FROM transactions
            WHERE status = 'posted'
            ORDER BY account_id, date DESC, created_at DESC
        """))
        
        transactions_by_account = {}
        
        for row in result:
            account_id = row.account_id
            transaction_date = row.date
            
            if account_id not in transactions_by_account:
                transactions_by_account[account_id] = {}
            
            if transaction_date not in transactions_by_account[account_id]:
                transactions_by_account[account_id][transaction_date] = []
            
            transactions_by_account[account_id][transaction_date].append({
                'amount': float(row.amount),
                'description': row.description,
                'running_balance': float(row.running_balance) if row.running_balance else None,
                'id': row.id
            })
        
        return transactions_by_account
    
    def reconstruct_daily_balances(self) -> List[Dict]:
        """Reconstruct daily balances by replaying transactions backwards"""
        print("🔄 Reconstructing daily balance history...")
        
        # Get accounts and date range
        accounts = self.get_accounts_with_current_balance()
        earliest_date, latest_date = self.get_date_range()
        
        print(f"📅 Date range: {earliest_date} to {latest_date}")
        print(f"🏦 Accounts: {len(accounts)}")
        
        # Get all transactions organized by account and date
        transactions_by_account = self.get_transactions_by_account_and_date()
        
        # Initialize balance tracking for each account
        account_balances = {}
        for account_id, account_info in accounts.items():
            account_balances[account_id] = account_info['current_balance']
        
        daily_balances = []
        current_date = latest_date
        
        print(f"⏪ Replaying transactions backwards...")
        
        while current_date >= earliest_date:
            # For each account, calculate balance at end of this day
            for account_id, account_info in accounts.items():
                end_of_day_balance = account_balances[account_id]
                transactions_today = []
                
                # If there are transactions for this account on this date
                if (account_id in transactions_by_account and 
                    current_date in transactions_by_account[account_id]):
                    
                    transactions_today = transactions_by_account[account_id][current_date]
                    
                    # Replay transactions backwards (subtract them from current balance)
                    # Since we're going backwards in time, we reverse the effect of each transaction
                    for transaction in transactions_today:
                        account_balances[account_id] -= transaction['amount']
                
                # Record the balance at end of day (after replaying transactions)
                # This represents what the balance was at the END of this day
                daily_balances.append({
                    'date': current_date,
                    'account_id': account_id,
                    'account_name': account_info['name'],
                    'account_type': account_info['type'],
                    'institution': account_info['institution'],
                    'end_of_day_balance': round(end_of_day_balance, 2),
                    'transaction_count': len(transactions_today),
                    'daily_change': round(sum(t['amount'] for t in transactions_today), 2) if transactions_today else 0.0
                })
            
            # Move to previous day
            current_date -= timedelta(days=1)
            
            # Progress indicator
            if current_date.day == 1 or current_date == earliest_date:
                print(f"  📆 Processed through {current_date}")
        
        print(f"✅ Generated {len(daily_balances)} daily balance records")
        return daily_balances
    
    def export_balance_history(self, output_format='csv') -> str:
        """Export balance history to file in pivot table format (one row per day)"""
        daily_balances = self.reconstruct_daily_balances()
        
        if not daily_balances:
            print("❌ No balance data to export")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(daily_balances)
        
        # Pivot to get one row per date with account balances as columns
        pivot_df = df.pivot_table(
            index='date',
            columns='account_name', 
            values='end_of_day_balance',
            aggfunc='first'
        ).reset_index()
        
        # Sort by date (newest first for easy analysis)
        pivot_df = pivot_df.sort_values('date', ascending=False)
        
        # Add total portfolio value column
        account_columns = [col for col in pivot_df.columns if col != 'date']
        pivot_df['total_portfolio'] = pivot_df[account_columns].sum(axis=1)
        
        # Add daily portfolio change column
        pivot_df = pivot_df.sort_values('date')  # Sort ascending for change calculation
        pivot_df['portfolio_change'] = pivot_df['total_portfolio'].diff()
        pivot_df = pivot_df.sort_values('date', ascending=False)  # Back to descending
        
        # Reorder columns: date, total_portfolio, portfolio_change, then individual accounts
        column_order = ['date', 'total_portfolio', 'portfolio_change'] + sorted(account_columns)
        pivot_df = pivot_df[column_order]
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if output_format == 'csv':
            filename = f"exports/daily_balances_pivot_{timestamp}.csv"
            pivot_df.to_csv(filename, index=False)
        else:
            filename = f"exports/daily_balances_pivot_{timestamp}.xlsx"
            pivot_df.to_excel(filename, index=False)
        
        print(f"💾 Exported balance history: {filename}")
        
        # Show summary
        total_days = len(pivot_df)
        date_range = f"{pivot_df['date'].min()} to {pivot_df['date'].max()}"
        accounts = [col for col in account_columns]
        
        print(f"\n📊 Summary:")
        print(f"  📅 Date range: {date_range}")
        print(f"  📆 Total days: {total_days:,}")
        print(f"  🏦 Accounts: {len(accounts)}")
        print(f"  💰 Portfolio range: ${pivot_df['total_portfolio'].min():.2f} to ${pivot_df['total_portfolio'].max():.2f}")
        
        # Show sample data (most recent days)
        print(f"\n📋 Sample data (most recent):")
        print(f"  {'Date':<12} | {'Portfolio':<12} | {'Change':<10} | Sample Account Balances")
        print(f"  {'-'*12} | {'-'*12} | {'-'*10} | {'-'*30}")
        
        for _, row in pivot_df.head(10).iterrows():
            change_str = f"{row['portfolio_change']:+8.2f}" if pd.notna(row['portfolio_change']) else "     --"
            # Show first 2 account balances as examples
            sample_accounts = []
            for acc in accounts[:2]:
                if pd.notna(row[acc]):
                    sample_accounts.append(f"{acc}: ${row[acc]:.0f}")
            sample_str = ", ".join(sample_accounts)
            
            print(f"  {row['date']} | ${row['total_portfolio']:>10.2f} | {change_str} | {sample_str}")
        
        return filename
    
    def create_balance_query(self) -> str:
        """Generate SQL query for balance reconstruction (for reference)"""
        return """
        -- Daily Balance Reconstruction Query
        -- This recreates the balance history by replaying transactions
        
        WITH RECURSIVE date_series AS (
            -- Generate all dates from earliest to latest transaction
            SELECT 
                MIN(date) as date_val,
                MAX(date) as max_date
            FROM transactions
            WHERE status = 'posted'
            
            UNION ALL
            
            SELECT 
                date_val + INTERVAL 1 DAY,
                max_date
            FROM date_series
            WHERE date_val < max_date
        ),
        
        daily_transactions AS (
            SELECT 
                ds.date_val as date,
                a.id as account_id,
                a.name as account_name,
                a.type as account_type,
                a.balance_amount as current_balance,
                COALESCE(SUM(t.amount), 0) as daily_change,
                COUNT(t.id) as transaction_count
            FROM date_series ds
            CROSS JOIN accounts a
            LEFT JOIN transactions t ON t.account_id = a.id 
                AND t.date = ds.date_val 
                AND t.status = 'posted'
            WHERE a.status = 'open'
            GROUP BY ds.date_val, a.id, a.name, a.type, a.balance_amount
        )
        
        SELECT 
            date,
            account_id,
            account_name,
            account_type,
            -- Calculate balance by working backwards from current balance
            current_balance - SUM(daily_change) OVER (
                PARTITION BY account_id 
                ORDER BY date DESC 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) + daily_change as end_of_day_balance,
            daily_change,
            transaction_count
        FROM daily_transactions
        ORDER BY date DESC, account_name;
        """
    
    def close(self):
        """Close database connection"""
        self.session.close()


def main():
    """Main execution function"""
    import sys
    
    # Database configuration
    mysql_url = os.getenv('DATABASE_URL', 'mysql+pymysql://teller_user:teller_pass@localhost:3306/teller_db')
    
    if mysql_url == 'mysql+pymysql://teller_user:teller_pass@localhost:3306/teller_db':
        print("⚠️  Using default DATABASE_URL")
    
    # Parse arguments
    output_format = 'csv'
    if len(sys.argv) > 1 and sys.argv[1] in ['csv', 'excel']:
        output_format = sys.argv[1]
    
    try:
        # Ensure exports directory exists
        os.makedirs('exports', exist_ok=True)
        
        builder = BalanceHistoryBuilder(mysql_url)
        
        # Generate and export balance history
        filename = builder.export_balance_history(output_format)
        
        if filename:
            print(f"\n💡 Usage ideas:")
            print(f"  📈 Create charts showing balance trends over time")
            print(f"  📊 Analyze spending patterns by comparing daily changes")
            print(f"  🔍 Identify unusual balance fluctuations")
            print(f"  📅 Track monthly/quarterly financial progress")
        
        # Show the SQL query for reference
        print(f"\n🔍 For reference, here's the SQL approach:")
        print("(This Python method is more reliable than pure SQL for this complex calculation)")
        
        builder.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    # Install pandas if needed
    try:
        import pandas as pd
    except ImportError:
        print("📦 Installing pandas for data export...")
        os.system("poetry add pandas openpyxl")
        import pandas as pd
    
    import sys
    sys.exit(main())