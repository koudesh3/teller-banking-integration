"""Teller API client implementation"""

import base64
from typing import List, Optional

import httpx
from pydantic import ValidationError

from .config import TellerConfig
from .models import Account, AccountBalance, Institution, Transaction


class TellerError(Exception):
    """Custom exception for Teller API errors"""
    
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class TellerClient:
    """Teller API client for banking operations"""
    
    def __init__(self, config: TellerConfig):
        self.config = config
        
        # Prepare client kwargs
        client_kwargs = {
            "base_url": config.base_url,
            "headers": self._get_auth_headers(),
            "timeout": 30.0
        }
        
        # Add certificate if provided
        if config.cert_file and config.key_file:
            client_kwargs["cert"] = (config.cert_file, config.key_file)
        
        self.client = httpx.Client(**client_kwargs)
    
    def _get_auth_headers(self) -> dict:
        """Get authentication headers for API requests"""
        auth_string = f"{self.config.access_token}:"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        return {
            "Authorization": f"Basic {encoded_auth}",
            "Content-Type": "application/json"
        }
    
    def _handle_response(self, response: httpx.Response) -> dict:
        """Handle API response and check for errors"""
        try:
            data = response.json()
        except Exception:
            response.raise_for_status()
            return {}
        
        # Check for Teller API errors
        if isinstance(data, dict) and "error" in data:
            error_info = data["error"]
            raise TellerError(
                code=error_info.get("code", "unknown"),
                message=error_info.get("message", "Unknown error")
            )
        
        response.raise_for_status()
        return data
    
    def health_check(self) -> bool:
        """Check if Teller API is healthy"""
        try:
            response = self.client.get("/health")
            return response.status_code == 200
        except Exception:
            return False
    
    def get_institutions(self) -> List[Institution]:
        """Get list of supported institutions"""
        response = self.client.get("/institutions")
        data = self._handle_response(response)
        return [Institution(**item) for item in data]
    
    def get_accounts(self) -> List[Account]:
        """Get all accounts for the authenticated user"""
        response = self.client.get("/accounts")
        data = self._handle_response(response)
        
        accounts = []
        for account_data in data:
            try:
                # Get account balance
                balance = self.get_account_balance(account_data["id"])
                account_data["balance"] = balance.dict()
                accounts.append(Account(**account_data))
            except ValidationError as e:
                print(f"Validation error for account {account_data.get('id', 'unknown')}: {e}")
                continue
            except Exception as e:
                print(f"Error processing account {account_data.get('id', 'unknown')}: {e}")
                continue
        
        return accounts
    
    def get_account_balance(self, account_id: str) -> AccountBalance:
        """Get balance for a specific account"""
        # Get recent transactions to find running balance
        transactions = self.get_transactions(account_id, count=20)
        
        # Find the most recent transaction with running balance
        for transaction in transactions:
            if transaction.running_balance is not None:
                return AccountBalance(
                    currency="USD",
                    amount=float(transaction.running_balance)
                )
        
        # Default to 0 if no running balance found
        return AccountBalance(currency="USD", amount=0.0)
    
    def get_transactions(
        self,
        account_id: str,
        count: Optional[int] = None,
        latest: bool = False
    ) -> List[Transaction]:
        """Get transactions for a specific account"""
        if latest:
            # Use pagination to get ALL transactions
            return self.get_all_transactions(account_id)
        
        # For specific count, use original logic
        params = {}
        if count:
            params["count"] = count
        
        response = self.client.get(f"/accounts/{account_id}/transactions", params=params)
        data = self._handle_response(response)
        
        transactions = []
        for transaction_data in data:
            try:
                # Filter out pending transactions (as per original implementation)
                if transaction_data.get("status") == "pending":
                    continue
                transactions.append(Transaction(**transaction_data))
            except ValidationError as e:
                print(f"Validation error for transaction {transaction_data.get('id', 'unknown')}: {e}")
                continue
        
        return transactions
    
    def get_all_transactions(self, account_id: str) -> List[Transaction]:
        """Get ALL transactions for an account using pagination"""
        all_transactions = []
        page_size = 250  # Good balance between API calls and memory
        
        print(f"    Fetching all transactions (using {page_size} per page)...")
        
        while True:
            params = {"count": page_size}
            
            # Use cursor-based pagination if we have transactions
            if all_transactions:
                # Use the oldest transaction ID as cursor for next page
                last_transaction = all_transactions[-1]
                # Teller uses 'from_id' for pagination
                params["from_id"] = last_transaction.id
            
            try:
                response = self.client.get(f"/accounts/{account_id}/transactions", params=params)
                data = self._handle_response(response)
                
                if not data or len(data) == 0:
                    break  # No more transactions
                
                page_transactions = []
                for transaction_data in data:
                    try:
                        # Filter out pending transactions
                        if transaction_data.get("status") == "pending":
                            continue
                        
                        transaction = Transaction(**transaction_data)
                        
                        # Skip if we already have this transaction (cursor overlap)
                        if not any(t.id == transaction.id for t in all_transactions):
                            page_transactions.append(transaction)
                            
                    except ValidationError as e:
                        print(f"    Validation error for transaction {transaction_data.get('id', 'unknown')}: {e}")
                        continue
                
                if not page_transactions:
                    break  # No new transactions found
                
                all_transactions.extend(page_transactions)
                print(f"    Fetched {len(page_transactions)} transactions (total: {len(all_transactions)})")
                
                # If we got less than requested, we've reached the end
                if len(data) < page_size:
                    break
                    
            except Exception as e:
                print(f"    Error fetching page: {e}")
                break
        
        print(f"    ✓ Total transactions fetched: {len(all_transactions)}")
        return all_transactions
    
    def get_connection_status(self) -> dict:
        """Check connection status"""
        try:
            response = self.client.get("/accounts")
            data = self._handle_response(response)
            
            if not isinstance(data, list) or len(data) == 0:
                return {"status": "disconnected"}
            
            # Try to access first account to verify connection
            first_account = data[0]
            account_response = self.client.get(f"/accounts/{first_account['id']}")
            self._handle_response(account_response)
            
            return {"status": "connected"}
            
        except TellerError as e:
            if e.code == "disconnected":
                return {"status": "disconnected"}
            return {"status": "connected"}
        except Exception:
            return {"status": "connected"}
    
    def delete_accounts(self) -> None:
        """Delete/disconnect all accounts"""
        response = self.client.delete("/accounts")
        self._handle_response(response)
    
    def close(self):
        """Close the HTTP client"""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()