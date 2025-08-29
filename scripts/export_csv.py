#!/usr/bin/env python3
"""
CSV Export Script for Teller Data

Creates a new timestamped folder with one CSV file per table.
Perfect for analysis in Excel, Google Sheets, or other tools.
"""

import os
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


class TellerCSVExporter:
    def __init__(self, mysql_url: str):
        """Initialize CSV exporter with MySQL connection"""
        self.engine = create_engine(mysql_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    def export_table_to_csv(self, table_name: str, output_path: Path, custom_query: str = None) -> int:
        """Export a table to CSV and return row count"""
        if custom_query:
            query = custom_query
        else:
            query = f"SELECT * FROM {table_name}"
        
        result = self.session.execute(text(query))
        rows = result.fetchall()
        
        if not rows:
            print(f"  ⚠️  {table_name}: No data found")
            return 0
        
        # Get column names
        columns = list(result.keys())
        
        # Write to CSV
        csv_file = output_path / f"{table_name}.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow(columns)
            
            # Write data rows
            for row in rows:
                writer.writerow(row)
        
        print(f"  ✅ {table_name}.csv: {len(rows)} rows")
        return len(rows)
    
    def get_summary_stats(self) -> Dict:
        """Get summary statistics for the export"""
        stats = {}
        
        # Account counts
        result = self.session.execute(text("""
            SELECT type, COUNT(*) as count, COALESCE(SUM(balance_amount), 0) as total_balance
            FROM accounts 
            WHERE status = 'open'
            GROUP BY type
        """))
        account_data = {}
        for row in result:
            account_data[row[0]] = (row[1], row[2])
        stats['accounts_by_type'] = account_data
        
        # Transaction date range
        result = self.session.execute(text("""
            SELECT 
                MIN(date) as earliest_transaction,
                MAX(date) as latest_transaction,
                COUNT(*) as total_transactions
            FROM transactions
        """)).fetchone()
        stats['transaction_summary'] = {
            'earliest': result[0],
            'latest': result[1], 
            'total': result[2]
        }
        
        # Top categories
        result = self.session.execute(text("""
            SELECT category, COUNT(*) as count, SUM(ABS(amount)) as total_amount
            FROM transactions 
            WHERE category IS NOT NULL AND amount < 0
            GROUP BY category
            ORDER BY total_amount DESC
            LIMIT 10
        """))
        stats['top_spending_categories'] = list(result.fetchall())
        
        return stats
    
    def create_summary_file(self, output_path: Path, stats: Dict, export_info: Dict):
        """Create a summary text file with export details"""
        summary_file = output_path / "README.txt"
        
        with open(summary_file, 'w') as f:
            f.write(f"Teller Data Export\n")
            f.write(f"================\n\n")
            f.write(f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Export Path: {output_path}\n\n")
            
            f.write(f"Files Exported:\n")
            for table, count in export_info.items():
                f.write(f"  - {table}.csv: {count:,} rows\n")
            f.write(f"\n")
            
            # Account summary
            f.write(f"Account Summary:\n")
            for account_type, (count, balance) in stats['accounts_by_type'].items():
                f.write(f"  - {account_type.title()}: {count} accounts, ${balance:,.2f}\n")
            f.write(f"\n")
            
            # Transaction summary
            tx_summary = stats['transaction_summary']
            f.write(f"Transaction Summary:\n")
            f.write(f"  - Total: {tx_summary['total']:,} transactions\n")
            f.write(f"  - Date Range: {tx_summary['earliest']} to {tx_summary['latest']}\n")
            f.write(f"\n")
            
            # Top spending categories
            f.write(f"Top Spending Categories:\n")
            for category, count, amount in stats['top_spending_categories'][:5]:
                f.write(f"  - {category.title()}: {count} transactions, ${amount:,.2f}\n")
            f.write(f"\n")
            
            f.write(f"Usage:\n")
            f.write(f"  - Open CSV files in Excel, Google Sheets, or any spreadsheet app\n")
            f.write(f"  - transactions.csv contains all your transaction data\n")
            f.write(f"  - accounts.csv shows your account information\n")
            f.write(f"  - Use filters and pivot tables for analysis\n")
    
    def export_all(self, base_path: str = "exports") -> Path:
        """Export all tables to CSV files in a timestamped folder"""
        # Create timestamped export folder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(base_path) / f"teller_export_{timestamp}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        print(f"📊 Exporting Teller data to: {output_path}")
        
        export_info = {}
        
        # Export core tables
        tables = {
            'institutions': None,
            'accounts': None,
            'transactions': None,
            'sync_runs': None
        }
        
        for table_name in tables:
            try:
                row_count = self.export_table_to_csv(table_name, output_path)
                export_info[table_name] = row_count
            except Exception as e:
                print(f"  ❌ {table_name}: Error - {e}")
                export_info[table_name] = 0
        
        # Export custom views/queries
        print(f"\n📈 Exporting analysis views...")
        
        # Monthly spending summary
        monthly_spending_query = """
        SELECT 
            YEAR(t.date) as year,
            MONTH(t.date) as month,
            a.name as account_name,
            a.type as account_type,
            t.category,
            COUNT(*) as transaction_count,
            SUM(t.amount) as net_amount,
            SUM(ABS(t.amount)) as total_volume
        FROM transactions t
        JOIN accounts a ON t.account_id = a.id
        WHERE t.status = 'posted'
        GROUP BY YEAR(t.date), MONTH(t.date), a.id, a.name, a.type, t.category
        ORDER BY year DESC, month DESC, total_volume DESC
        """
        
        try:
            row_count = self.export_table_to_csv('monthly_spending', output_path, monthly_spending_query)
            export_info['monthly_spending'] = row_count
        except Exception as e:
            print(f"  ❌ monthly_spending: Error - {e}")
            export_info['monthly_spending'] = 0
        
        # Recent transactions with account names
        recent_transactions_query = """
        SELECT 
            t.date,
            a.name as account_name,
            i.name as bank_name,
            t.description,
            t.amount,
            t.category,
            t.counterparty_name,
            t.running_balance,
            t.status
        FROM transactions t
        JOIN accounts a ON t.account_id = a.id
        JOIN institutions i ON a.institution_id = i.id
        ORDER BY t.date DESC, t.created_at DESC
        LIMIT 1000
        """
        
        try:
            row_count = self.export_table_to_csv('recent_transactions', output_path, recent_transactions_query)
            export_info['recent_transactions'] = row_count
        except Exception as e:
            print(f"  ❌ recent_transactions: Error - {e}")
            export_info['recent_transactions'] = 0
        
        # Get summary statistics
        print(f"\n📋 Generating summary...")
        stats = self.get_summary_stats()
        
        # Create summary file
        self.create_summary_file(output_path, stats, export_info)
        
        print(f"\n✅ Export complete!")
        print(f"📁 Location: {output_path.absolute()}")
        print(f"📊 Files: {len([f for f in output_path.glob('*.csv')])} CSV files")
        print(f"📄 Summary: README.txt")
        
        return output_path
    
    def close(self):
        """Close database connection"""
        self.session.close()


def main():
    """Main execution function"""
    import sys
    
    # Database configuration
    mysql_url = os.getenv('DATABASE_URL', 'mysql+pymysql://teller_user:teller_pass@localhost:3306/teller_db')
    
    if mysql_url == 'mysql+pymysql://teller_user:teller_pass@localhost:3306/teller_db':
        print("⚠️  Using default DATABASE_URL. Set your MySQL connection:")
        print("   export DATABASE_URL='mysql+pymysql://user:password@localhost/your_db'")
    
    # Parse command line arguments
    output_dir = "exports"
    if len(sys.argv) > 1:
        output_dir = sys.argv[1]
    
    try:
        exporter = TellerCSVExporter(mysql_url)
        export_path = exporter.export_all(output_dir)
        exporter.close()
        
        print(f"\n🎉 Ready to analyze!")
        print(f"💡 Try opening transactions.csv in Excel or Google Sheets")
        print(f"📈 Use monthly_spending.csv for budget analysis")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())