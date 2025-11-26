"""
Square Service for Bakehouse ETL

Handles all Square API operations:
- Authentication (multi-location token support)
- Order fetching (single/multiple locations)
- Date range queries
- Connection testing

Key features:
- Location-specific API tokens (V1.1)
- Automatic pagination
- RFC3339 timestamp handling
- Error handling with retries
"""

import requests
import datetime as dt
from dateutil import tz
from typing import List, Dict, Tuple, Optional
import logging

from app.utils.config import config


# Configure logging
logger = logging.getLogger(__name__)


class SquareAPIError(Exception):
    """Custom exception for Square API errors."""
    pass


class SquareService:
    """
    Service for Square API interactions.
    
    Supports multiple locations with different API tokens due that each location
    has its own token 
    
    Usage:
        from app.services.square_service import square_service
        
        # Fetch orders for location with specific token
        orders = square_service.fetch_orders_by_date_range(
            location_id="L5WST6KFZBT10",
            location_db_id=2,  # Uses token for location 2
            start_date="2025-11-01",
            end_date="2025-11-30"
        )
    """
    
    def __init__(self):
        """
        Initialize Square service with default token and base URL.
        
        Location-specific tokens are loaded per-request from config.
        """
        self.base_url = "https://connect.squareup.com"
        
        # Default token (used as fallback)
        self.access_token = config.SQUARE_ACCESS_TOKEN
        
        # Default headers (for connection testing)
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        logger.info(f"SquareService initialized with environment: {config.ENVIRONMENT}")
        logger.info(f"Multi-location token support: ENABLED (V1.1)")
    
    
    def _get_headers_for_location(self, location_db_id: int) -> Dict[str, str]:
        """
        Get API headers with the appropriate token for a location.
        
        Uses location-specific token if available, falls back to default.
        
        Args:
            location_db_id: Database location ID (1, 2, or 3)
        
        Returns:
            Headers dict with appropriate Authorization token
        """
        # Get token mapping from config
        token_map = getattr(config, 'LOCATION_TOKEN_MAP', {})
        
        # Get location-specific token or fallback to default
        token = token_map.get(location_db_id, self.access_token)
        
        if token_map.get(location_db_id):
            logger.debug(f"Using location-specific token for location_db_id={location_db_id}")
        else:
            logger.debug(f"Using default token for location_db_id={location_db_id} (no specific token found)")
        
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    
    def day_window(
        self, 
        days_ago: int = 1, 
        tzname: str = "America/Chicago"
    ) -> Tuple[dt.datetime, dt.datetime, str, str]:
        """
        Generate time window for a specific day in local timezone.
        
        Returns both datetime objects and RFC3339 formatted strings.
        
        Args:
            days_ago: Number of days in the past (0 = today, 1 = yesterday)
            tzname: Timezone name (default: America/Chicago)
        
        Returns:
            Tuple: (start_dt, end_dt, start_rfc3339, end_rfc3339)
        """
        local_tz = tz.gettz(tzname)
        now_local = dt.datetime.now(local_tz)
        
        start = (now_local - dt.timedelta(days=days_ago)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end = start + dt.timedelta(days=1)
        
        start_rfc = start.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
        end_rfc = end.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
        
        logger.debug(f"Generated day window: {start_rfc} to {end_rfc}")
        return start, end, start_rfc, end_rfc
    
    
    def fetch_orders_by_date_range(
        self,
        location_id: str,
        location_db_id: int,
        start_date: str,
        end_date: str,
        state: str = "COMPLETED",
        test: bool = False
    ) -> List[Dict]:
        """
        Fetch orders for a specific location and date range.
        
        Uses location-specific API token based on location_db_id (V1.1).
        
        Args:
            location_id: Square location ID (e.g., "L5WST6KFZBT10")
            location_db_id: Database location ID (1, 2, or 3) - determines which token to use
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            state: Order state to filter by (default: "COMPLETED")
            test: If True, fetch max 2 pages (200 orders) for testing
        
        Returns:
            List of order objects from Square API
        
        Raises:
            SquareAPIError: If API request fails
            ValueError: If date format is invalid
        """
        try:
            local_tz = tz.gettz("America/Chicago")
            
            # Parse dates
            start_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)
            
            end_dt = dt.datetime.strptime(end_date, "%Y-%m-%d")
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=local_tz)
            
            # Convert to RFC3339 UTC
            start_rfc = start_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
            end_rfc = end_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
            
        except ValueError as e:
            raise ValueError(
                f"Invalid date format. Expected 'YYYY-MM-DD', got start_date='{start_date}', end_date='{end_date}'"
            ) from e
        
        # Get location-specific headers (V1.1)
        headers = self._get_headers_for_location(location_db_id)
        
        logger.info(
            f"Fetching orders for location {location_id} (db_id={location_db_id}) "
            f"from {start_date} to {end_date}"
            f"{' (TEST MODE - max 2 pages/200 orders)' if test else ''}"
        )
        logger.debug(f"RFC3339 range: {start_rfc} to {end_rfc}")
        
        url = f"{self.base_url}/v2/orders/search"
        
        orders = []
        cursor = None
        page_count = 0
        max_pages = 2 if test else None
        
        while True:
            page_count += 1
            
            # Stop if reached max pages (test mode)
            if max_pages and page_count > max_pages:
                logger.info(f"Reached max pages limit ({max_pages}), stopping")
                break
            
            body = {
                "location_ids": [location_id],
                "query": {
                    "filter": {
                        "date_time_filter": {
                            "closed_at": {
                                "start_at": start_rfc,
                                "end_at": end_rfc
                            }
                        },
                        "state_filter": {
                            "states": [state]
                        }
                    },
                    "sort": {
                        "sort_field": "CLOSED_AT",
                        "sort_order": "ASC"
                    }
                },
                "limit": 100
            }
            
            if cursor:
                body["cursor"] = cursor
            
            try:
                logger.debug(f"Making POST request to {url} (page {page_count})")
                response = requests.post(url, headers=headers, json=body)
                response.raise_for_status()
                
                data = response.json()
                page_orders = data.get("orders", [])
                orders.extend(page_orders)
                
                logger.info(
                    f"Fetched page {page_count} with {len(page_orders)} orders "
                    f"(total: {len(orders)})"
                )
                
                cursor = data.get("cursor")
                
                if not cursor:
                    logger.info("No more pages, pagination complete")
                    break
                    
            except requests.exceptions.HTTPError as e:
                error_msg = f"Square API HTTP error: {e.response.status_code}"
                try:
                    error_data = e.response.json()
                    error_msg += f" - {error_data}"
                except:
                    error_msg += f" - {e.response.text}"
                
                logger.error(error_msg)
                raise SquareAPIError(error_msg) from e
                
            except requests.exceptions.RequestException as e:
                error_msg = f"Square API request failed: {str(e)}"
                logger.error(error_msg)
                raise SquareAPIError(error_msg) from e
        
        logger.info(
            f"Successfully fetched {len(orders)} orders for location {location_id} "
            f"({start_date} to {end_date}) across {page_count} pages"
        )
        return orders
    
    
    def fetch_orders_by_date(
        self,
        location_id: str,
        location_db_id: int,
        days_ago: int = 0,
        state: str = "COMPLETED",
        test: bool = False
    ) -> List[Dict]:
        """
        Fetch orders for a specific location and date.
        
        Convenience wrapper around fetch_orders_by_date_range.
        
        Args:
            location_id: Square location ID
            location_db_id: Database location ID (1, 2, or 3)
            days_ago: Number of days in the past (0 = today, 1 = yesterday)
            state: Order state to filter by (default: "COMPLETED")
            test: If True, only fetch first 2 pages (for testing)
        
        Returns:
            List of order objects from Square API
        """
        start_local, end_local, _, _ = self.day_window(days_ago)
        
        start_date = start_local.strftime('%Y-%m-%d')
        end_date = (end_local - dt.timedelta(seconds=1)).strftime('%Y-%m-%d')
        
        return self.fetch_orders_by_date_range(
            location_id=location_id,
            location_db_id=location_db_id,
            start_date=start_date,
            end_date=end_date,
            state=state,
            test=test
        )
    
    
    def fetch_multiple_locations(
        self,
        locations: List[Dict[str, any]],
        days_ago: int = 0,
        state: str = "COMPLETED",
        test: bool = False
    ) -> Dict[str, List[Dict]]:
        """
        Fetch orders for multiple locations for the same date.
        
        Args:
            locations: List of location dicts with 'square_id' and 'id' (db_id)
            days_ago: Number of days in the past
            state: Order state to filter by
            test: If True, only fetch first 2 pages per location
        
        Returns:
            Dict mapping location square_id to list of orders
        """
        results = {}
        
        for location in locations:
            square_id = location.get('square_id')
            db_id = location.get('id')
            
            if not square_id or db_id is None:
                logger.error(f"Invalid location dict: {location}")
                continue
            
            try:
                orders = self.fetch_orders_by_date(
                    location_id=square_id,
                    location_db_id=db_id,
                    days_ago=days_ago,
                    state=state,
                    test=test
                )
                results[square_id] = orders
                
            except SquareAPIError as e:
                logger.error(f"Failed to fetch orders for location {square_id}: {e}")
                results[square_id] = []
        
        return results
    
    
    def test_connection(self, location_db_id: Optional[int] = None) -> bool:
        """
        Test Square API connection and authentication.
        
        Args:
            location_db_id: Optional location ID to test specific token
                           If None, uses default token
        
        Returns:
            True if connection successful
        """
        try:
            # Use location-specific headers if provided
            if location_db_id is not None:
                headers = self._get_headers_for_location(location_db_id)
                logger.info(f"Testing connection with token for location_db_id={location_db_id}")
            else:
                headers = self.headers
                logger.info("Testing connection with default token")
            
            url = f"{self.base_url}/v2/locations"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            locations = response.json().get("locations", [])
            logger.info(f"Connection test successful. Found {len(locations)} locations.")
            
            for loc in locations:
                logger.info(f"  - {loc.get('name')} (ID: {loc.get('id')})")
            
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False


# Singleton instance
square_service = SquareService()


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    """
    Test Square service.
    
    Usage:
        python app/services/square_service.py
    """
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    from datetime import timedelta
    
    print("\n" + "#"*60)
    print("# SQUARE SERVICE TEST SUITE (V1.1)")
    print("#"*60)
    
    results = []
    
    # =================================================================
    # TEST 1: Connection Test (Default Token)
    # =================================================================
    print("\n" + "="*60)
    print("TEST 1: Connection Test (Default Token)")
    print("="*60)
    
    try:
        result = square_service.test_connection()
        if result:
            print("✅ Default token connection PASSED")
            results.append(("Connection Test (Default)", True))
        else:
            print("❌ Default token connection FAILED")
            results.append(("Connection Test (Default)", False))
    except Exception as e:
        print(f"❌ Connection test ERROR: {e}")
        results.append(("Connection Test (Default)", False))
    
    # =================================================================
    # TEST 2: Connection Test (Location-Specific Token)
    # =================================================================
    print("\n" + "="*60)
    print("TEST 2: Connection Test (Location 2 Token)")
    print("="*60)
    
    try:
        result = square_service.test_connection(location_db_id=2)
        if result:
            print("✅ Location 2 token connection PASSED")
            results.append(("Connection Test (Location 2)", True))
        else:
            print("❌ Location 2 token connection FAILED")
            results.append(("Connection Test (Location 2)", False))
    except Exception as e:
        print(f"❌ Connection test ERROR: {e}")
        results.append(("Connection Test (Location 2)", False))
    
    # =================================================================
    # TEST 3: Fetch Orders with Location Token
    # =================================================================
    print("\n" + "="*60)
    print("TEST 3: Fetch Orders with Location Token (Test Mode)")
    print("="*60)
    
    try:
        # IMPORTANT: Replace with your actual location ID
        location_id = "L5WST6KFZBT10"
        location_db_id = 2  # Your current location
        
        orders = square_service.fetch_orders_by_date(
            location_id=location_id,
            location_db_id=location_db_id,
            days_ago=0,
            test=True
        )
        
        print(f"✅ Fetched {len(orders)} orders using location-specific token")
        
        if orders:
            print(f"\nSample order:")
            print(f"  ID: {orders[0].get('id')}")
            print(f"  Closed at: {orders[0].get('closed_at')}")
            print(f"  Line items: {len(orders[0].get('line_items', []))}")
        
        print("✅ Location token fetch PASSED")
        results.append(("Fetch with Location Token", True))
            
    except SquareAPIError as e:
        print(f"❌ Square API error: {e}")
        results.append(("Fetch with Location Token", False))
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        results.append(("Fetch with Location Token", False))
    
    # =================================================================
    # TEST 4: Date Range with Location Token
    # =================================================================
    print("\n" + "="*60)
    print("TEST 4: Fetch Date Range with Location Token")
    print("="*60)
    
    try:
        location_id = "L5WST6KFZBT10"
        location_db_id = 2
        
        end_date = dt.datetime.now().strftime('%Y-%m-%d')
        start_date = (dt.datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        orders = square_service.fetch_orders_by_date_range(
            location_id=location_id,
            location_db_id=location_db_id,
            start_date=start_date,
            end_date=end_date,
            test=True
        )
        
        print(f"✅ Fetched {len(orders)} orders from {start_date} to {end_date}")
        print("✅ Date range with location token PASSED")
        results.append(("Date Range with Token", True))
        
    except SquareAPIError as e:
        print(f"❌ Square API error: {e}")
        results.append(("Date Range with Token", False))
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        results.append(("Date Range with Token", False))
    
    # =================================================================
    # TEST 5: Day Window Helper
    # =================================================================
    print("\n" + "="*60)
    print("TEST 5: Day Window Helper")
    print("="*60)
    
    try:
        start, end, start_rfc, end_rfc = square_service.day_window(days_ago=0)
        
        print(f"Today's window:")
        print(f"  Local start: {start}")
        print(f"  UTC start: {start_rfc}")
        
        duration = (end - start).total_seconds()
        expected_duration = 24 * 60 * 60
        
        if duration == expected_duration:
            print(f"✅ Window duration correct: {duration/3600} hours")
            print("✅ Day window PASSED")
            results.append(("Day Window Helper", True))
        else:
            print(f"❌ ERROR: Expected 24 hours, got {duration/3600} hours")
            results.append(("Day Window Helper", False))
        
    except Exception as e:
        print(f"❌ Error: {e}")
        results.append(("Day Window Helper", False))
    
    # =================================================================
    # SUMMARY
    # =================================================================
    print("\n" + "="*60)
    print("TEST SUMMARY (V1.1)")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print("="*60)
    print(f"Results: {passed}/{total} tests passed")
    print("="*60)
    print("\nV1.1 Changes:")
    print("  ✓ Added location_db_id parameter to fetch methods")
    print("  ✓ Location-specific token support via _get_headers_for_location()")
    print("  ✓ Fallback to default token if location token not found")
    print("="*60)
    
    exit(0 if passed == total else 1)