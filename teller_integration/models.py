"""Teller API data models"""

from datetime import date
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class DetailCategory(str, Enum):
    ACCOMMODATION = "accommodation"
    ADVERTISING = "advertising"
    BAR = "bar"
    CHARITY = "charity"
    CLOTHING = "clothing"
    DINING = "dining"
    EDUCATION = "education"
    ELECTRONICS = "electronics"
    ENTERTAINMENT = "entertainment"
    FUEL = "fuel"
    GENERAL = "general"
    GROCERIES = "groceries"
    HEALTH = "health"
    HOME = "home"
    INCOME = "income"
    INSURANCE = "insurance"
    INVESTMENT = "investment"
    LOAN = "loan"
    OFFICE = "office"
    PHONE = "phone"
    SERVICE = "service"
    SHOPPING = "shopping"
    SOFTWARE = "software"
    SPORT = "sport"
    TAX = "tax"
    TRANSPORT = "transport"
    TRANSPORTATION = "transportation"
    UTILITIES = "utilities"


class TransactionStatus(str, Enum):
    POSTED = "posted"
    PENDING = "pending"


class AccountType(str, Enum):
    DEPOSITORY = "depository"
    CREDIT = "credit"


class AccountStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


class TransactionDetails(BaseModel):
    category: Optional[DetailCategory] = None
    processing_status: str
    counterparty: Optional[dict] = None


class Institution(BaseModel):
    id: str
    name: str


class Transaction(BaseModel):
    id: str
    account_id: str
    amount: str
    date: str
    description: str
    status: TransactionStatus
    type: str
    running_balance: Optional[str] = None
    details: TransactionDetails
    links: dict


class AccountBalance(BaseModel):
    currency: str
    amount: float


class Account(BaseModel):
    id: str
    name: str
    currency: str
    type: AccountType
    subtype: str
    status: AccountStatus
    last_four: str
    enrollment_id: str
    institution: Institution
    balance: Optional[AccountBalance] = None
    links: dict