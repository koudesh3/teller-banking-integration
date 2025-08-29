#!/usr/bin/env python3
"""
Portfolio Balance History with Simulated S&P 500 Investments

Tracks Robinhood transfers and simulates investing 100% in S&P 500 on transfer dates.
Creates a complete portfolio view including simulated stock holdings.
"""

import os
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from decimal import Decimal

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import yfinance as yf


class PortfolioWithSP500:
    def __init__(self, mysql_url: str):
        """Initialize portfolio tracker with S&P 500 simulation"""
        self.engine = create_engine(mysql_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    def find_robinhood_transfers(self) -> List[Dict]:
        """Find all transfers to Robinhood and their amounts"""
        result = self.session.execute(text("""
            SELECT 
                date,
                description,
                ABS(amount) as transfer_amount,
                account_id,
                a.name as account_name
            FROM transactions t
            JOIN accounts a ON t.account_id = a.id
            WHERE LOWER(t.description) LIKE '%robinhood%'
                AND t.amount < 0  -- Outgoing transfers (negative amounts)
                AND t.status = 'posted'
            ORDER BY date ASC
        """))
        
        transfers = []
        for row in result:
            transfers.append({
                'date': row.date,
                'description': row.description,
                'amount': float(row.transfer_amount),
                'account_id': row.account_id,
                'account_name': row.account_name
            })
        
        return transfers
    
    def get_sp500_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Get S&P 500 price data for date range"""
        print(f"📈 Fetching S&P 500 data from {start_date} to {end_date}...")
        
        # Use ^GSPC (S&P 500 index) or SPY (SPDR S&P 500 ETF)
        ticker = yf.Ticker("SPY")  # Using SPY for more reliable data
        
        # Get historical data
        hist = ticker.history(start=start_date, end=end_date + timedelta(days=1))
        
        if hist.empty:
            print("⚠️  No S&P 500 data found for date range")
            return pd.DataFrame()
        
        # Reset index to get dates as a column
        hist.reset_index(inplace=True)
        hist['Date'] = hist['Date'].dt.date
        
        return hist[['Date', 'Close']].rename(columns={'Date': 'date', 'Close': 'price'})
    
    def calculate_sp500_portfolio(self, transfers: List[Dict], sp500_data: pd.DataFrame) -> Dict[date, float]:
        """Calculate S&P 500 portfolio value for each day"""
        if not transfers or sp500_data.empty:
            return {}
        
        print(f"💰 Calculating S&P 500 portfolio from {len(transfers)} transfers...")
        
        # Convert to dict for faster lookup
        sp500_prices = {row['date']: row['price'] for _, row in sp500_data.iterrows()}
        
        total_shares = 0.0
        share_history = {}  # date -> total_shares_owned
        
        # Calculate shares purchased on each transfer date
        for transfer in transfers:
            transfer_date = transfer['date']
            transfer_amount = transfer['amount']
            
            # Find the closest price (same day or next trading day)
            price = None
            search_date = transfer_date
            for _ in range(5):  # Search up to 5 days ahead for trading day
                if search_date in sp500_prices:
                    price = sp500_prices[search_date]
                    break
                search_date += timedelta(days=1)
            
            if price:
                shares_bought = transfer_amount / price
                total_shares += shares_bought
                
                print(f"  📅 {transfer_date}: ${transfer_amount:,.2f} → {shares_bought:.4f} shares @ ${price:.2f}")
            else:
                print(f"  ⚠️  {transfer_date}: No price data found for ${transfer_amount:,.2f} transfer")
                continue
            
            # Record total shares owned from this date forward
            share_history[transfer_date] = total_shares
        
        # Now calculate portfolio value for all dates
        portfolio_values = {}
        current_shares = 0.0
        
        # Get date range for all S&P 500 data
        all_dates = sorted(sp500_prices.keys())
        
        for date_val in all_dates:
            # Update shares if there was a transfer on this date
            if date_val in share_history:
                current_shares = share_history[date_val]
            
            # Calculate portfolio value
            if current_shares > 0:
                price = sp500_prices[date_val]
                portfolio_values[date_val] = current_shares * price
            else:
                portfolio_values[date_val] = 0.0
        
        print(f"✅ Generated S&P 500 portfolio values for {len(portfolio_values)} days")
        print(f"📊 Total shares owned: {current_shares:.4f}")
        if portfolio_values:
            latest_value = max(portfolio_values.values())
            print(f"💰 Latest portfolio value: ${latest_value:,.2f}")
        
        return portfolio_values
    
    def get_accounts_with_current_balance(self) -> Dict[str, Dict]:
        """Get all accounts with their current balances (from existing code)"""
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
        """Get all transactions organized by account and date (from existing code)"""
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
    
    def reconstruct_daily_balances_with_sp500(self) -> pd.DataFrame:
        """Reconstruct daily balances including simulated S&P 500 portfolio"""
        print("🔄 Building complete portfolio with S&P 500 simulation...")
        
        # Get basic data
        accounts = self.get_accounts_with_current_balance()
        earliest_date, latest_date = self.get_date_range()
        transactions_by_account = self.get_transactions_by_account_and_date()
        
        print(f"📅 Date range: {earliest_date} to {latest_date}")
        print(f"🏦 Bank accounts: {len(accounts)}")
        
        # Find Robinhood transfers
        transfers = self.find_robinhood_transfers()
        print(f"💸 Found {len(transfers)} Robinhood transfers totaling ${sum(t['amount'] for t in transfers):,.2f}")
        
        # Get S&P 500 data if we have transfers
        sp500_portfolio = {}
        if transfers:
            # Get a bit more data to ensure we have prices
            start_date = min(t['date'] for t in transfers) - timedelta(days=30)
            end_date = latest_date + timedelta(days=30)
            
            sp500_data = self.get_sp500_data(start_date, end_date)
            sp500_portfolio = self.calculate_sp500_portfolio(transfers, sp500_data)
        
        # Reconstruct bank account balances (existing logic)
        print(f"⏪ Reconstructing bank account balances...")
        account_balances = {}
        for account_id, account_info in accounts.items():
            account_balances[account_id] = account_info['current_balance']
        
        daily_records = []
        current_date = latest_date
        
        while current_date >= earliest_date:
            # Calculate bank account balances for this day
            day_balances = {'date': current_date}
            
            for account_id, account_info in accounts.items():
                end_of_day_balance = account_balances[account_id]
                
                # Process any transactions on this date
                if (account_id in transactions_by_account and 
                    current_date in transactions_by_account[account_id]):
                    
                    transactions_today = transactions_by_account[account_id][current_date]
                    
                    # Replay transactions backwards
                    for transaction in transactions_today:
                        account_balances[account_id] -= transaction['amount']
                
                # Store the balance
                day_balances[account_info['name']] = round(end_of_day_balance, 2)
            
            # Add S&P 500 portfolio value - use most recent available price
            sp500_value = 0.0
            if sp500_portfolio:
                # Find the most recent S&P 500 value (carry forward from last trading day)
                search_date = current_date
                while search_date >= min(sp500_portfolio.keys()) and sp500_value == 0.0:
                    if search_date in sp500_portfolio:
                        sp500_value = sp500_portfolio[search_date]
                        break
                    search_date -= timedelta(days=1)
            
            day_balances['Robinhood_SP500'] = round(sp500_value, 2)
            
            # Calculate total portfolio (bank accounts + S&P 500)
            account_total = sum(day_balances[acc['name']] for acc in accounts.values())
            day_balances['total_portfolio'] = round(account_total + day_balances['Robinhood_SP500'], 2)
            
            daily_records.append(day_balances)
            
            # Move to previous day
            current_date -= timedelta(days=1)
            
            # Progress indicator
            if current_date.day == 1 or current_date == earliest_date:
                print(f"  📆 Processed through {current_date}")
        
        # Convert to DataFrame and sort
        df = pd.DataFrame(daily_records)
        df = df.sort_values('date', ascending=False)
        
        # Add daily change calculation
        df = df.sort_values('date')  # Sort ascending for diff calculation
        df['portfolio_change'] = df['total_portfolio'].diff()
        df = df.sort_values('date', ascending=False)  # Back to descending
        
        print(f"✅ Generated complete portfolio history: {len(df)} days")
        
        return df
    
    def export_complete_portfolio(self, output_format='csv') -> str:
        """Export complete portfolio including S&P 500 simulation"""
        df = self.reconstruct_daily_balances_with_sp500()
        
        if df.empty:
            print("❌ No portfolio data to export")
            return None
        
        # Reorder columns nicely
        account_columns = [col for col in df.columns if col not in ['date', 'total_portfolio', 'portfolio_change', 'Robinhood_SP500']]
        column_order = ['date', 'total_portfolio', 'portfolio_change', 'Robinhood_SP500'] + sorted(account_columns)
        df = df[column_order]
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if output_format == 'csv':
            filename = f"exports/complete_portfolio_{timestamp}.csv"
            df.to_csv(filename, index=False)
        else:
            filename = f"exports/complete_portfolio_{timestamp}.xlsx"
            df.to_excel(filename, index=False)
        
        print(f"💾 Exported complete portfolio: {filename}")
        
        # Show summary
        total_days = len(df)
        date_range = f"{df['date'].min()} to {df['date'].max()}"
        portfolio_range = f"${df['total_portfolio'].min():.2f} to ${df['total_portfolio'].max():.2f}"
        
        latest_row = df.iloc[0]  # First row (most recent date)
        sp500_value = latest_row['Robinhood_SP500']
        bank_value = latest_row['total_portfolio'] - sp500_value
        
        print(f"\n📊 Complete Portfolio Summary:")
        print(f"  📅 Date range: {date_range}")
        print(f"  📆 Total days: {total_days:,}")
        print(f"  💰 Portfolio range: {portfolio_range}")
        print(f"  🏦 Current bank accounts: ${bank_value:,.2f}")
        print(f"  📈 Current S&P 500 simulation: ${sp500_value:,.2f}")
        print(f"  🎯 Total current value: ${latest_row['total_portfolio']:,.2f}")
        
        # Show recent data sample
        print(f"\n📋 Recent portfolio values:")
        print(f"  {'Date':<12} | {'Total':<12} | {'Change':<10} | {'S&P500':<12} | {'Banks':<12}")
        print(f"  {'-'*12} | {'-'*12} | {'-'*10} | {'-'*12} | {'-'*12}")
        
        for _, row in df.head(10).iterrows():
            change_str = f"{row['portfolio_change']:+8.2f}" if pd.notna(row['portfolio_change']) else "     --"
            bank_val = row['total_portfolio'] - row['Robinhood_SP500']
            
            print(f"  {row['date']} | ${row['total_portfolio']:>10.2f} | {change_str} | ${row['Robinhood_SP500']:>10.2f} | ${bank_val:>10.2f}")
        
        return filename
    
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
        
        portfolio = PortfolioWithSP500(mysql_url)
        
        # Generate and export complete portfolio
        filename = portfolio.export_complete_portfolio(output_format)
        
        if filename:
            print(f"\n💡 Analysis Ideas:")
            print(f"  📈 Compare S&P 500 performance vs bank account growth")
            print(f"  📊 See total portfolio value over time")
            print(f"  🎯 Track investment timing and market performance")
            print(f"  📅 Analyze asset allocation changes over time")
        
        portfolio.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())