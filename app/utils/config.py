"""
Configuration Management for Bakehouse-ETL

This module loads environment variables and provides a centralized configuration, it
it also  handles swithching between local and production modes based on the ENVIRONMENT variable.
WHY THIS EXISTS:
- Centralized configuration (Single Source of Truth)
- Environment-based switching (dev vs prod)
- Type validation and error handling
- Prevents hardcoded values throughout the codebase

DESIGN PATTERN:
- Singleton-like behavior (one config instance for entire app)
- Lazy loading (only loads when accessed)
- Fail-fast validation (errors on startup, not during execution)
"""

import os
from typing import Optional
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


class Config:
    """
    Configuration class that provides environment-based settings.
    
    This class reads from .env file and exposes clean configuration
    properties. It automatically selects local or production database
    based on the ENVIRONMENT variable.
    
    Usage for testing:
        from app.utils.config import config
        
        print(config.DB_HOST)  # Automatically uses LOCAL or PROD based on ENVIRONMENT
        print(config.SQUARE_ACCESS_TOKEN)
    """
    
    def __init__(self):
        """
        Initialize configuration by reading environment variables.
        
        Validates that critical variables exist and selects the
        appropriate database configuration.
        """
        # Get environment (default to development for safety)
        self.ENVIRONMENT = os.getenv('ENVIRONMENT', 'development').lower()
        
        # Validate environment value
        if self.ENVIRONMENT not in ['development', 'production']:
            raise ValueError(
                f"Invalid ENVIRONMENT value: '{self.ENVIRONMENT}'. "
                f"Must be 'development' or 'production'"
            )
        
        # Load database configuration based on environment
        if self.ENVIRONMENT == 'development':
            self._load_local_database_config()
        else:
            self._load_production_database_config()
        
        # Load Square API configuration
        self._load_square_config()
        
        # Load application settings
        self._load_app_settings()
    
    
    def _load_local_database_config(self):
        """Load local (development) database configuration."""
        self.DB_HOST = os.getenv('LOCAL_DB_HOST')
        self.DB_PORT = int(os.getenv('LOCAL_DB_PORT', '5432'))
        self.DB_NAME = os.getenv('LOCAL_DB_NAME')
        self.DB_USER = os.getenv('LOCAL_DB_USER')
        self.DB_PASSWORD = os.getenv('LOCAL_DB_PASSWORD')
        
        # Validate required fields
        self._validate_database_config('LOCAL')
    
    
    def _load_production_database_config(self):
        """
        Load production (AWS RDS) database configuration.
        
        Supports two naming conventions for flexibility:
        1. Prefixed: PROD_DB_HOST, PROD_DB_NAME, etc. (local .env files)
        2. Non-prefixed: DB_HOST, DB_NAME, etc. (GitHub Actions, simpler setup)
        
        Tries prefixed version first, falls back to non-prefixed.
        This makes it compatible with both local development and CI/CD environments.
        """
        # Try PROD_* first, fall back to non-prefixed version
        self.DB_HOST = os.getenv('PROD_DB_HOST') or os.getenv('DB_HOST')
        self.DB_PORT = int(os.getenv('PROD_DB_PORT') or os.getenv('DB_PORT', '5432'))
        self.DB_NAME = os.getenv('PROD_DB_NAME') or os.getenv('DB_NAME')
        self.DB_USER = os.getenv('PROD_DB_USER') or os.getenv('DB_USER')
        self.DB_PASSWORD = os.getenv('PROD_DB_PASSWORD') or os.getenv('DB_PASSWORD')
        
        # Validate required fields
        self._validate_database_config('PROD')
    
    
    def _validate_database_config(self, env: str):
        """
        Validate that all required database configuration is present.
        
        Args:
            env is either 'LOCAL' or 'PROD' 
        
        Raises:
            ValueError: If any required configuration is missing
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
        """Load Square API configuration."""
        self.SQUARE_ACCESS_TOKEN = os.getenv('SQUARE_ACCESS_TOKEN')
        
        # ====================
        # SQUARE API CONFIGURATION
        # ====================
        self.SQUARE_API_BASE_URL: str = "https://connect.squareup.com/v2"
        
        if not self.SQUARE_ACCESS_TOKEN:
            raise ValueError(
                "Missing SQUARE_ACCESS_TOKEN in .env file. "
                "This is required for fetching data from Square API."
            )
    
    
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
            str: PostgreSQL connection URL 
        
        """
        
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@"
            f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
    
    
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


# Create singleton instance
config = Config()


# ============================================================================
# TESTING & VALIDATION
# ============================================================================

if __name__ == "__main__":
    """
    Run this file directly to validate configuration.
    
    Usage:
        # Test development environment
        python app/utils/config.py
        
        # Test production environment
        ENVIRONMENT=production python app/utils/config.py
    """
    print("=" * 70)
    print("BAKEHOUSE ETL - CONFIGURATION TEST")
    print("=" * 70)
    
    try:
        print(f"\nCurrent Environment: {config.ENVIRONMENT}")
        print(f"\nDatabase Configuration:")
        print(f"  Host: {config.DB_HOST}")
        print(f"  Port: {config.DB_PORT}")
        print(f"  Database: {config.DB_NAME}")
        print(f"  User: {config.DB_USER}")
        print(f"  Password: {'*' * len(config.DB_PASSWORD)} (hidden)")
        
        print(f"\nSquare API:")
        token_preview = config.SQUARE_ACCESS_TOKEN[:20] + "..." if len(config.SQUARE_ACCESS_TOKEN) > 20 else config.SQUARE_ACCESS_TOKEN
        print(f"  Token: {token_preview} (configured ✓)")
        
        print(f"\nApplication:")
        print(f"  Log Level: {config.LOG_LEVEL}")
        
        print(f"\nConnection URL:")
        # Mask password in URL for security
        url = config.get_database_url()
        masked_url = url.replace(config.DB_PASSWORD, "****")
        print(f"  {masked_url}")
        
        print(f"\nEnvironment Checks:")
        print(f"  Is Development: {config.is_development()}")
        print(f"  Is Production: {config.is_production()}")
        print("\n" + "=" * 70)
        print("Configuration is valid! ✅")
        print("=" * 70)
        
    except ValueError as e:
        print("\n" + "=" * 70)
        print("❌ CONFIGURATION ERROR")
        print("=" * 70)
        print(f"\n{str(e)}")
        print("\nPlease check your .env file and ensure all required variables are set.")
        print("=" * 70)
        exit(1)
    
    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ UNEXPECTED ERROR")
        print("=" * 70)
        print(f"\n{str(e)}")
        print("=" * 70)
        exit(1)