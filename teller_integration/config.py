"""Configuration handling for Teller integration"""

import os
from typing import Optional

from pydantic import BaseModel
from dotenv import load_dotenv


class TellerConfig(BaseModel):
    """Teller API configuration"""
    
    access_token: str
    base_url: str = "https://api.teller.io"
    application_id: Optional[str] = None
    environment: Optional[str] = None
    signing_secret: Optional[str] = None
    cert_file: Optional[str] = None
    key_file: Optional[str] = None

    @classmethod
    def from_env(cls) -> "TellerConfig":
        """Load configuration from environment variables"""
        load_dotenv()
        
        access_token = os.getenv("TELLER_ACCESS_TOKEN")
        if not access_token:
            raise ValueError("TELLER_ACCESS_TOKEN environment variable is required")
        
        return cls(
            access_token=access_token,
            base_url=os.getenv("TELLER_BASE_URL", "https://api.teller.io"),
            application_id=os.getenv("TELLER_APPLICATION_ID"),
            environment=os.getenv("TELLER_ENVIRONMENT"),
            signing_secret=os.getenv("TELLER_SIGNING_SECRET"),
            cert_file=os.getenv("TELLER_CERT_FILE"),
            key_file=os.getenv("TELLER_KEY_FILE"),
        )