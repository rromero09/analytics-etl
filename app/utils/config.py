"""
Configuration Management for ETL Application

Loads environment variables and provides centralized config.
Handles local vs production modes based on ENVIRONMENT variable.

Design:
- Singleton-like (one config instance for entire app)
- Fail-fast validation (errors on startup, not during execution)
- Multi-location token support
"""

import os
from typing import Optional, Dict
from dotenv import load_dotenv
from urllib.parse import quote_plus 


# Load environment variables from .env file
load_dotenv()


class Config:
    """
    Configuration class for environment-based settings.
    
    Reads from .env file and exposes clean configuration properties.
    Automatically selects local or production database based on ENVIRONMENT.
    
    Usage:
        from app.utils.config import config
        
        print(config.DB_HOST)
        print(config.SQUARE_ACCESS_TOKEN)
        print(config.LOCATION_TOKEN_MAP) 
    """
    
    def __init__(self):
        """Initialize configuration by reading environment variables."""
        # Get environment (default to development)
        self.ENVIRONMENT = os.getenv('ENVIRONMENT', 'development').lower()
        
        # Validate environment value
        if self.ENVIRONMENT not in ['development', 'production']:
            raise ValueError(
                f"Invalid ENVIRONMENT value: '{self.ENVIRONMENT}'. "
                f"Must be 'development' or 'production'"
            )
        
        # Load configurations
        if self.ENVIRONMENT == 'development':
            self._load_local_database_config()
        else:
            self._load_production_database_config()
        
        self._load_square_config()
        self._load_app_settings()
    
    
    def _load_local_database_config(self):
        """Load local (development) database configuration."""
        self.DB_HOST = os.getenv('LOCAL_DB_HOST')
        self.DB_PORT = int(os.getenv('LOCAL_DB_PORT', '5432'))
        self.DB_NAME = os.getenv('LOCAL_DB_NAME')
        self.DB_USER = os.getenv('LOCAL_DB_USER')
        self.DB_PASSWORD = os.getenv('LOCAL_DB_PASSWORD')
        
        self._validate_database_config('LOCAL')
    
    
    def _load_production_database_config(self):
        """
        Load production database configuration.
   
        1. Prefixed: PROD_DB_HOST, PROD_DB_NAME, etc.
        2. Non-prefixed: DB_HOST, DB_NAME, etc.
        
        PROD-prefixed variables take precedence if both are set.
        """
        self.DB_HOST = os.getenv('PROD_DB_HOST') or os.getenv('DB_HOST')
        self.DB_PORT = int(os.getenv('PROD_DB_PORT') or os.getenv('DB_PORT', '5432'))
        self.DB_NAME = os.getenv('PROD_DB_NAME') or os.getenv('DB_NAME')
        self.DB_USER = os.getenv('PROD_DB_USER') or os.getenv('DB_USER')
        self.DB_PASSWORD = os.getenv('PROD_DB_PASSWORD') or os.getenv('DB_PASSWORD')
        
        self._validate_database_config('PROD')
    
    
    def _validate_database_config(self, env: str):
        """
        Validate required database configuration is present.
        
        Args:
            env: Either 'LOCAL' or 'PROD'
        
        Raises:
            ValueError: If any required env missing
        """
        missing = []
        
        if not self.DB_HOST:
            missing.append(f'{env}_DB_HOST or DB_HOST')
        if not self.DB_NAME:
            missing.append(f'{env}_DB_NAME or DB_NAME')
        if not self.DB_USER:
            missing.append(f'{env}_DB_USER or DB_USER')
        if not self.DB_PASSWORD:
            missing.append(f'{env}_DB_PASSWORD or DB_PASSWORD')
        
        if missing:
            raise ValueError(
                f"Missing required {env} database configuration in .env file: "
                f"{', '.join(missing)}"
            )
    
    
    def _load_square_config(self):
        """
        Load Square API configuration.
        
        - Loads location-specific tokens (SQUARE_ACCESS_TOKEN_WRIGLEYVILLE, etc.)
        - Creates LOCATION_TOKEN_MAP for multi-location support
        - Backwards compatible (location tokens are optional)
        """
        # Default token (required)
        self.SQUARE_ACCESS_TOKEN = os.getenv('SQUARE_ACCESS_TOKEN')
        
        if not self.SQUARE_ACCESS_TOKEN:
            raise ValueError(
                "Missing SQUARE_ACCESS_TOKEN in .env file. "
                "This is required for fetching data from Square API."
            )
        
        # Base URL
        self.SQUARE_API_BASE_URL: str = "https://connect.squareup.com/v2"
        
        # load secundary locations 
        token_wrigleyville = os.getenv('SQUARE_ACCESS_TOKEN_WRIGLEYVILLE')
        token_southport = os.getenv('SQUARE_ACCESS_TOKEN_SOUTHPORT')
        
        # Create location token mapping
        # Maps database location_id → Square API token
        self.LOCATION_TOKEN_MAP: Dict[int, str] = {
            2: self.SQUARE_ACCESS_TOKEN  # Location 2 (current/default)
        }
        
        # Add location-specific tokens if provided
        if token_wrigleyville:
            self.LOCATION_TOKEN_MAP[1] = token_wrigleyville  # Location 1 (Wrigleyville)
        
        if token_southport:
            self.LOCATION_TOKEN_MAP[3] = token_southport  # Location 3 (Southport)
    
    
    def _load_app_settings(self):
        """Load application-level settings."""
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
        
        # Validate log level
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.LOG_LEVEL not in valid_levels:
            raise ValueError(
                f"Invalid LOG_LEVEL: '{self.LOG_LEVEL}'. "
                f"Must be one of: {', '.join(valid_levels)}"
            )
    
    
    def get_database_url(self) -> str:
        """
        Get PostgreSQL connection URL.
        
        Returns:
            PostgreSQL connection URL with SSL for production
        """
        encoded_password = quote_plus(self.DB_PASSWORD)
        
        base_url = (
            f"postgresql://{self.DB_USER}:{encoded_password}@"
            f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
        
        # Add SSL requirement for production
        if self.is_production():
            base_url += "?sslmode=require" 
        
        return base_url
    
    
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.ENVIRONMENT == 'development'
    
    
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.ENVIRONMENT == 'production'
    
    
    def __repr__(self) -> str:
        """String representation (hides sensitive data)."""
        return (
            f"<Config environment={self.ENVIRONMENT} "
            f"db={self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}>"
        )


# Singleton instance
config = Config() # Single instance for app-wide use or "pythonic singleton"
# be aware 
# from app.utils.config import config  ✅ CORRECT (Singleton)

# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    """
    Validate configuration.
    
    Usage:
        # Test development
        python app/utils/config.py
        
        # Test production
        ENVIRONMENT=production python app/utils/config.py
    """
    print("=" * 70)
    print("BAKEHOUSE ETL - CONFIGURATION TEST (V1.1)")
    print("=" * 70)
    
    try:
        print(f"\nEnvironment: {config.ENVIRONMENT}")
        
        print(f"\nDatabase:")
        print(f"  Host: {config.DB_HOST}")
        print(f"  Port: {config.DB_PORT}")
        print(f"  Database: {config.DB_NAME}")
        print(f"  User: {config.DB_USER}")
        print(f"  Password: {'*' * len(config.DB_PASSWORD)} (hidden)")
        
        print(f"\nSquare API:")
        token_preview = config.SQUARE_ACCESS_TOKEN[:20] + "..."
        print(f"  Default Token: {token_preview}")
        
        print(f"\nSquare API - Multi-Location Tokens (V1.1):")
        for loc_id, token in config.LOCATION_TOKEN_MAP.items():
            token_preview = token[:20] + "..." if len(token) > 20 else token
            location_name = {1: "Wrigleyville", 2: "Current", 3: "Southport"}.get(loc_id, "Unknown")
            print(f"  Location {loc_id} ({location_name}): {token_preview}")
        
        if len(config.LOCATION_TOKEN_MAP) == 1:
            print(f"  ⚠️  Only default token configured (location 2)")
            print(f"  💡 Add SQUARE_ACCESS_TOKEN_WRIGLEYVILLE and SQUARE_ACCESS_TOKEN_SOUTHPORT")
            print(f"     to .env file for multi-location support")
        else:
            print(f"  ✓ Multi-location tokens configured!")
        
        print(f"\nApplication:")
        print(f"  Log Level: {config.LOG_LEVEL}")
        
        print(f"\nConnection URL:")
        url = config.get_database_url()
        masked_url = url.replace(config.DB_PASSWORD, "****")
        print(f"  {masked_url}")
        
        print(f"\nEnvironment Checks:")
        print(f"  Is Development: {config.is_development()}")
        print(f"  Is Production: {config.is_production()}")
        
        print("\n" + "=" * 70)
        print("Configuration Valid! ✅")
        print("=" * 70)
        
    except ValueError as e:
        print("\n" + "=" * 70)
        print("❌ CONFIGURATION ERROR")
        print("=" * 70)
        print(f"\n{str(e)}")
        print("\nCheck your .env file and ensure all required variables are set.")
        print("=" * 70)
        exit(1)
    
    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ UNEXPECTED ERROR")
        print("=" * 70)
        print(f"\n{str(e)}")
        print("=" * 70)
        exit(1)     