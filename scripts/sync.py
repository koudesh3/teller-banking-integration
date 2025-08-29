#!/usr/bin/env python3
"""
Incremental Database Loader for Teller Data

Only pulls new/updated transactions since last sync.
Maintains sync state to avoid duplicate work.
"""

import os
from datetime import datetime, timedelta, date
from typing import List, Optional, Dict, Tuple

import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from teller_integration import TellerClient
from teller_integration.config import TellerConfig
from teller_integration.models import Account, Transaction


class IncrementalTellerLoader:
    def __init__(self, mysql_url: str):
        """Initialize incremental loader with MySQL connection"""
        self.engine = create_engine(mysql_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    def create_tables(self):
        """Create database tables if they don't exist"""
        tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS institutions (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_name (name)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sync_runs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP NULL,
                status ENUM('running', 'completed', 'failed') DEFAULT 'running',
                sync_type ENUM('full', 'incremental') DEFAULT 'incremental',
                accounts_synced INT DEFAULT 0,
                transactions_synced INT DEFAULT 0,
                new_transactions INT DEFAULT 0,
                updated_transactions INT DEFAULT 0,
                error_message TEXT NULL,
                INDEX idx_status (status),
                INDEX idx_sync_type (sync_type),
                INDEX idx_started_at (started_at)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id VARCHAR(50) PRIMARY KEY,
                institution_id VARCHAR(50) NOT NULL,
                enrollment_id VARCHAR(100) NOT NULL,
                name VARCHAR(255) NOT NULL,
                type ENUM('depository', 'credit') NOT NULL,
                subtype VARCHAR(50) NOT NULL,
                status ENUM('open', 'closed') DEFAULT 'open',
                currency VARCHAR(3) DEFAULT 'USD',
                last_four VARCHAR(4) NOT NULL,
                balance_amount DECIMAL(15,2) NULL,
                balance_currency VARCHAR(3) DEFAULT 'USD',
                balance_updated_at TIMESTAMP NULL,
                last_transaction_date DATE NULL,
                last_transaction_id VARCHAR(100) NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                last_synced_at TIMESTAMP NULL,
                FOREIGN KEY (institution_id) REFERENCES institutions(id) ON DELETE CASCADE,
                INDEX idx_institution (institution_id),
                INDEX idx_enrollment (enrollment_id),
                INDEX idx_type (type),
                INDEX idx_status (status),
                INDEX idx_last_synced (last_synced_at),
                INDEX idx_last_transaction_date (last_transaction_date)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id VARCHAR(100) PRIMARY KEY,
                account_id VARCHAR(50) NOT NULL,
                amount DECIMAL(15,2) NOT NULL,
                date DATE NOT NULL,
                description TEXT NOT NULL,
                status ENUM('posted', 'pending') NOT NULL,
                type VARCHAR(50) NOT NULL,
                running_balance DECIMAL(15,2) NULL,
                category ENUM(
                    'accommodation', 'advertising', 'bar', 'charity', 'clothing',
                    'dining', 'education', 'electronics', 'entertainment', 'fuel',
                    'general', 'groceries', 'health', 'home', 'income', 'insurance',
                    'investment', 'loan', 'office', 'phone', 'service', 'shopping',
                    'software', 'sport', 'tax', 'transport', 'transportation', 'utilities'
                ) NULL,
                processing_status VARCHAR(50) NULL,
                counterparty_name VARCHAR(255) NULL,
                counterparty_type ENUM('organization', 'person') NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE,
                INDEX idx_account_date (account_id, date DESC),
                INDEX idx_date (date),
                INDEX idx_amount (amount),
                INDEX idx_status (status),
                INDEX idx_category (category),
                INDEX idx_type (type),
                INDEX idx_description (description(100)),
                INDEX idx_counterparty (counterparty_name(50)),
                INDEX idx_account_category_date (account_id, category, date),
                INDEX idx_category_amount (category, amount),
                INDEX idx_monthly_summary (account_id, date, category)
            )
            """
        ]
        
        with self.engine.connect() as conn:
            for sql in tables_sql:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                except Exception as e:
                    print(f"Warning creating table: {e}")
        
        print("✓ Database tables created/verified")
        
    def get_last_sync_info(self) -> Optional[Dict]:
        """Get information about the last successful sync"""
        result = self.session.execute(
            text("""
                SELECT id, completed_at, accounts_synced, transactions_synced
                FROM sync_runs 
                WHERE status = 'completed' 
                ORDER BY completed_at DESC 
                LIMIT 1
            """)
        ).fetchone()
        
        if result:
            return {
                "sync_id": result[0],
                "completed_at": result[1],
                "accounts_synced": result[2],
                "transactions_synced": result[3]
            }
        return None
    
    def get_account_sync_state(self, account_id: str) -> Dict:
        """Get sync state for a specific account"""
        result = self.session.execute(
            text("""
                SELECT last_transaction_date, last_transaction_id, last_synced_at
                FROM accounts 
                WHERE id = :account_id
            """),
            {"account_id": account_id}
        ).fetchone()
        
        if result:
            return {
                "last_transaction_date": result[0],
                "last_transaction_id": result[1],
                "last_synced_at": result[2]
            }
        return {
            "last_transaction_date": None,
            "last_transaction_id": None,
            "last_synced_at": None
        }
    
    def start_sync_run(self, sync_type: str = 'incremental') -> int:
        """Start a new sync run"""
        result = self.session.execute(
            text("""
                INSERT INTO sync_runs (started_at, status, sync_type) 
                VALUES (NOW(), 'running', :sync_type)
            """),
            {"sync_type": sync_type}
        )
        self.session.commit()
        
        sync_run_id = result.lastrowid
        last_sync = self.get_last_sync_info()
        
        if sync_type == 'incremental' and last_sync:
            print(f"✓ Started incremental sync run #{sync_run_id}")
            print(f"  Last sync: {last_sync['completed_at']} ({last_sync['transactions_synced']} transactions)")
        else:
            print(f"✓ Started full sync run #{sync_run_id}")
        
        return sync_run_id
    
    def complete_sync_run(self, sync_run_id: int, stats: Dict):
        """Mark sync run as completed with detailed stats"""
        self.session.execute(
            text("""
                UPDATE sync_runs 
                SET completed_at = NOW(), 
                    status = 'completed',
                    accounts_synced = :accounts,
                    transactions_synced = :total_transactions,
                    new_transactions = :new_transactions,
                    updated_transactions = :updated_transactions
                WHERE id = :sync_id
            """),
            {
                "accounts": stats['accounts_synced'],
                "total_transactions": stats['total_transactions'],
                "new_transactions": stats['new_transactions'],
                "updated_transactions": stats['updated_transactions'],
                "sync_id": sync_run_id
            }
        )
        self.session.commit()
        print(f"✓ Completed sync run #{sync_run_id}")
        print(f"  New transactions: {stats['new_transactions']}")
        print(f"  Updated transactions: {stats['updated_transactions']}")
    
    def update_account_sync_state(self, account_id: str, latest_transaction: Optional[Transaction]):
        """Update account sync state after processing transactions"""
        if latest_transaction:
            self.session.execute(
                text("""
                    UPDATE accounts 
                    SET last_transaction_date = :date,
                        last_transaction_id = :transaction_id,
                        last_synced_at = NOW()
                    WHERE id = :account_id
                """),
                {
                    "account_id": account_id,
                    "date": latest_transaction.date,
                    "transaction_id": latest_transaction.id
                }
            )
        else:
            # No transactions, just update sync time
            self.session.execute(
                text("UPDATE accounts SET last_synced_at = NOW() WHERE id = :account_id"),
                {"account_id": account_id}
            )
    
    def load_institution(self, institution_id: str, institution_name: str):
        """Load institution (upsert)"""
        self.session.execute(
            text("""
                INSERT INTO institutions (id, name, updated_at) 
                VALUES (:id, :name, NOW())
                ON DUPLICATE KEY UPDATE 
                    name = VALUES(name),
                    updated_at = NOW()
            """),
            {"id": institution_id, "name": institution_name}
        )
    
    def load_account(self, account: Account):
        """Load account with sync state tracking"""
        balance_amount = None
        balance_updated_at = None
        
        if account.balance:
            balance_amount = account.balance.amount
            balance_updated_at = datetime.now()
        
        # Ensure institution exists
        self.load_institution(account.institution.id, account.institution.name)
        
        self.session.execute(
            text("""
                INSERT INTO accounts (
                    id, institution_id, enrollment_id, name, type, subtype, 
                    status, currency, last_four, balance_amount, balance_currency,
                    balance_updated_at, updated_at
                ) VALUES (
                    :id, :institution_id, :enrollment_id, :name, :type, :subtype,
                    :status, :currency, :last_four, :balance_amount, :balance_currency,
                    :balance_updated_at, NOW()
                )
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    status = VALUES(status),
                    balance_amount = VALUES(balance_amount),
                    balance_currency = VALUES(balance_currency),
                    balance_updated_at = VALUES(balance_updated_at),
                    updated_at = NOW()
            """),
            {
                "id": account.id,
                "institution_id": account.institution.id,
                "enrollment_id": account.enrollment_id,
                "name": account.name,
                "type": account.type.value,
                "subtype": account.subtype,
                "status": account.status.value,
                "currency": account.currency,
                "last_four": account.last_four,
                "balance_amount": balance_amount,
                "balance_currency": account.balance.currency if account.balance else "USD",
                "balance_updated_at": balance_updated_at
            }
        )
    
    def transaction_exists(self, transaction_id: str) -> bool:
        """Check if transaction already exists in database"""
        result = self.session.execute(
            text("SELECT 1 FROM transactions WHERE id = :id LIMIT 1"),
            {"id": transaction_id}
        ).fetchone()
        return result is not None
    
    def load_transaction(self, transaction: Transaction) -> str:
        """
        Load transaction (upsert) and return action taken
        Returns: 'new', 'updated', or 'skipped'
        """
        exists = self.transaction_exists(transaction.id)
        
        # Parse counterparty data
        counterparty_name = None
        counterparty_type = None
        
        if transaction.details.counterparty:
            counterparty_name = transaction.details.counterparty.get('name')
            counterparty_type = transaction.details.counterparty.get('type')
        
        self.session.execute(
            text("""
                INSERT INTO transactions (
                    id, account_id, amount, date, description, status, type,
                    running_balance, category, processing_status, counterparty_name,
                    counterparty_type, created_at, updated_at
                ) VALUES (
                    :id, :account_id, :amount, :date, :description, :status, :type,
                    :running_balance, :category, :processing_status, :counterparty_name,
                    :counterparty_type, NOW(), NOW()
                )
                ON DUPLICATE KEY UPDATE
                    amount = VALUES(amount),
                    description = VALUES(description),
                    status = VALUES(status),
                    running_balance = VALUES(running_balance),
                    category = VALUES(category),
                    processing_status = VALUES(processing_status),
                    counterparty_name = VALUES(counterparty_name),
                    counterparty_type = VALUES(counterparty_type),
                    updated_at = NOW()
            """),
            {
                "id": transaction.id,
                "account_id": transaction.account_id,
                "amount": float(transaction.amount),
                "date": transaction.date,
                "description": transaction.description,
                "status": transaction.status.value,
                "type": transaction.type,
                "running_balance": float(transaction.running_balance) if transaction.running_balance else None,
                "category": transaction.details.category.value if transaction.details.category else None,
                "processing_status": transaction.details.processing_status,
                "counterparty_name": counterparty_name,
                "counterparty_type": counterparty_type
            }
        )
        
        return 'updated' if exists else 'new'
    
    def should_do_full_sync(self) -> bool:
        """Determine if we should do a full sync instead of incremental"""
        last_sync = self.get_last_sync_info()
        
        if not last_sync:
            print("ℹ️  No previous sync found - doing full sync")
            return True
        
        # If last sync was more than 7 days ago, do full sync
        days_since_sync = (datetime.now() - last_sync['completed_at']).days
        if days_since_sync > 7:
            print(f"ℹ️  Last sync was {days_since_sync} days ago - doing full sync")
            return True
        
        return False
    
    def get_new_transactions_for_account(
        self, 
        teller_client: TellerClient, 
        account_id: str
    ) -> List[Transaction]:
        """Get only new transactions for an account since last sync"""
        sync_state = self.get_account_sync_state(account_id)
        
        # Get latest transactions from Teller
        all_transactions = teller_client.get_transactions(account_id, latest=True)
        
        if not sync_state['last_transaction_date']:
            # First time syncing this account - return all transactions
            return all_transactions
        
        # Filter transactions newer than last sync
        last_date = sync_state['last_transaction_date']
        new_transactions = []
        
        for transaction in all_transactions:
            transaction_date = datetime.strptime(transaction.date, '%Y-%m-%d').date()
            
            # Include transactions after last sync date
            if transaction_date > last_date:
                new_transactions.append(transaction)
            # Also include transactions from the last sync date that we haven't seen
            elif (transaction_date == last_date and 
                  transaction.id != sync_state['last_transaction_id'] and
                  not self.transaction_exists(transaction.id)):
                new_transactions.append(transaction)
        
        return new_transactions
    
    def sync_teller_data(self, force_full: bool = False):
        """Perform incremental sync of Teller data"""
        
        # Determine sync type
        if force_full or self.should_do_full_sync():
            sync_type = 'full'
        else:
            sync_type = 'incremental'
        
        sync_run_id = self.start_sync_run(sync_type)
        
        stats = {
            'accounts_synced': 0,
            'total_transactions': 0,
            'new_transactions': 0,
            'updated_transactions': 0
        }
        
        try:
            # Load Teller configuration
            config = TellerConfig.from_env()
            
            with TellerClient(config) as client:
                print("📊 Loading accounts...")
                accounts = client.get_accounts()
                
                # Update accounts info
                for account in accounts:
                    self.load_account(account)
                    stats['accounts_synced'] += 1
                
                self.session.commit()
                print(f"✓ Updated {len(accounts)} accounts")
                
                print(f"📋 Loading {sync_type} transactions...")
                
                for account in accounts:
                    if sync_type == 'full':
                        print(f"  Full sync: {account.name}")
                        transactions = client.get_transactions(account.id, latest=True)
                        print(f"  ✓ {account.name}: {len(transactions)} transactions")
                    else:
                        transactions = self.get_new_transactions_for_account(client, account.id)
                        if transactions:
                            print(f"  Incremental: {account.name} - {len(transactions)} new transactions")
                        else:
                            print(f"  Incremental: {account.name} - no new transactions")
                    
                    account_new = 0
                    account_updated = 0
                    latest_transaction = None
                    
                    for transaction in transactions:
                        action = self.load_transaction(transaction)
                        if action == 'new':
                            account_new += 1
                        elif action == 'updated':
                            account_updated += 1
                        
                        # Track latest transaction for sync state
                        if not latest_transaction or transaction.date > latest_transaction.date:
                            latest_transaction = transaction
                    
                    # Update account sync state
                    if transactions:
                        self.update_account_sync_state(account.id, latest_transaction)
                    else:
                        self.update_account_sync_state(account.id, None)
                    
                    stats['new_transactions'] += account_new
                    stats['updated_transactions'] += account_updated
                    stats['total_transactions'] += len(transactions)
                    
                    # Commit after each account
                    self.session.commit()
                
                self.complete_sync_run(sync_run_id, stats)
                return stats
                
        except Exception as e:
            # Mark sync run as failed
            self.session.execute(
                text("""
                    UPDATE sync_runs 
                    SET status = 'failed', error_message = :error
                    WHERE id = :sync_id
                """),
                {"error": str(e), "sync_id": sync_run_id}
            )
            self.session.commit()
            raise
    
    def get_sync_history(self, limit: int = 10):
        """Get recent sync history"""
        result = self.session.execute(
            text("""
                SELECT 
                    id, started_at, completed_at, status, sync_type,
                    accounts_synced, new_transactions, updated_transactions
                FROM sync_runs 
                ORDER BY started_at DESC 
                LIMIT :limit
            """),
            {"limit": limit}
        )
        return list(result.fetchall())
    
    def close(self):
        """Close database connection"""
        self.session.close()


def main():
    """Main execution function"""
    import sys
    
    # Parse command line arguments
    force_full = '--full' in sys.argv
    
    # Database configuration
    mysql_url = os.getenv('DATABASE_URL', 'mysql+pymysql://user:password@localhost/teller_db')
    
    if mysql_url == 'mysql+pymysql://user:password@localhost/teller_db':
        print("⚠️  Please set your DATABASE_URL environment variable:")
        print("   export DATABASE_URL='mysql+pymysql://user:password@localhost/your_db'")
        return 1
    
    try:
        loader = IncrementalTellerLoader(mysql_url)
        
        # Create tables if they don't exist
        loader.create_tables()
        
        # Show sync history
        print("📈 Recent sync history:")
        history = loader.get_sync_history(5)
        for run in history:
            status_emoji = "✅" if run[3] == 'completed' else "❌" if run[3] == 'failed' else "🔄"
            print(f"  {status_emoji} #{run[0]} - {run[1]} - {run[4]} - {run[6]} new, {run[7]} updated")
        
        print(f"\n🔄 Starting sync...")
        
        stats = loader.sync_teller_data(force_full=force_full)
        
        print(f"\n📊 Sync Summary:")
        print(f"  Accounts: {stats['accounts_synced']}")
        print(f"  New transactions: {stats['new_transactions']}")
        print(f"  Updated transactions: {stats['updated_transactions']}")
        print(f"  Total processed: {stats['total_transactions']}")
        
        loader.close()
        print(f"\n✅ Incremental sync completed successfully!")
        
        if stats['new_transactions'] == 0 and stats['updated_transactions'] == 0:
            print("💡 No new data found. All transactions are up to date!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())