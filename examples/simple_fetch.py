#!/usr/bin/env python3
"""
Teller Integration Script

This script connects to the Teller API to pull account information and transactions.
Requires a TELLER_ACCESS_TOKEN environment variable to be set.
"""

import json
import sys
from typing import Optional

from teller_integration import TellerClient
from teller_integration.config import TellerConfig


def print_json(data, indent=2):
    """Pretty print JSON data"""
    print(json.dumps(data, indent=indent, default=str))


def main():
    """Main execution function"""
    try:
        # Load configuration from environment
        config = TellerConfig.from_env()
        print(f"✓ Configuration loaded successfully")
        print(f"  Base URL: {config.base_url}")
        
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("\nPlease ensure TELLER_ACCESS_TOKEN is set in your environment or .env file")
        return 1
    
    # Initialize Teller client
    with TellerClient(config) as client:
        try:
            # Health check
            print(f"\n🏥 Health check...")
            if client.health_check():
                print("✓ Teller API is healthy")
            else:
                print("⚠️  Teller API health check failed")
            
            # Check connection status
            print(f"\n🔗 Connection status...")
            status = client.get_connection_status()
            print(f"✓ Status: {status['status']}")
            
            if status["status"] == "disconnected":
                print("❌ Account is disconnected. Please reconnect through Teller Connect.")
                return 1
            
            # Get accounts
            print(f"\n💰 Fetching accounts...")
            accounts = client.get_accounts()
            print(f"✓ Found {len(accounts)} account(s)")
            
            for i, account in enumerate(accounts, 1):
                print(f"\n--- Account {i}: {account.name} ---")
                print(f"ID: {account.id}")
                print(f"Type: {account.type.value} ({account.subtype})")
                print(f"Institution: {account.institution.name}")
                print(f"Last Four: {account.last_four}")
                print(f"Status: {account.status.value}")
                if account.balance:
                    print(f"Balance: {account.balance.currency} {account.balance.amount}")
            
            # Get transactions for each account
            print(f"\n📋 Fetching transactions...")
            all_transactions = []
            
            for account in accounts:
                print(f"\n--- Transactions for {account.name} ---")
                try:
                    transactions = client.get_transactions(account.id, latest=True)
                    print(f"✓ Found {len(transactions)} transaction(s)")
                    all_transactions.extend(transactions)
                    
                    # Show first few transactions as example
                    for transaction in transactions[:5]:  # Show first 5
                        print(f"  {transaction.date}: {transaction.description} - ${transaction.amount}")
                        if transaction.running_balance:
                            print(f"    Running Balance: ${transaction.running_balance}")
                    
                    if len(transactions) > 5:
                        print(f"    ... and {len(transactions) - 5} more")
                        
                except Exception as e:
                    print(f"❌ Error fetching transactions for {account.name}: {e}")
            
            print(f"\n📊 Summary:")
            print(f"  Total accounts: {len(accounts)}")
            print(f"  Total transactions: {len(all_transactions)}")
            
            # Optional: Save data to files
            save_data = input(f"\n💾 Save data to JSON files? (y/N): ").lower().strip()
            if save_data == 'y':
                # Save accounts
                accounts_data = [account.dict() for account in accounts]
                with open('accounts.json', 'w') as f:
                    json.dump(accounts_data, f, indent=2, default=str)
                print("✓ Accounts saved to accounts.json")
                
                # Save transactions
                transactions_data = [transaction.dict() for transaction in all_transactions]
                with open('transactions.json', 'w') as f:
                    json.dump(transactions_data, f, indent=2, default=str)
                print("✓ Transactions saved to transactions.json")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return 1
    
    print(f"\n✅ Script completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())