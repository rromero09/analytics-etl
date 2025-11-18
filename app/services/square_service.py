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
    Service class for interacting with Square API.
    
    This class handles all Square API operations including authentication,
    request construction, and response parsing.
    
    Usage:
        from app.services.square_service import square_service
        
        # Fetch orders for a date range
        orders = square_service.fetch_orders_by_date_range(..)
        
        
        # Fetch today's orders (quick testing)
        orders = square_service.fetch_orders_by_date(...)
        
    """
    
    def __init__(self):
        """
        Initialize the Square service.
        
        Sets up authentication headers and base URL from config.
        """
        self.base_url = "https://connect.squareup.com"
        
        self.headers = {
            "Authorization": f"Bearer {config.SQUARE_ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        logger.info(f"SquareService initialized with environment: {config.ENVIRONMENT}")
    
    
    def day_window(
        self, 
        days_ago: int = 1, 
        tzname: str = "America/Chicago"
    ) -> Tuple[dt.datetime, dt.datetime, str, str]:
        """
        Generate time window for a specific day in local timezone.
        
        This matches the exact implementation from the a working Google Colab test.
        Returns both datetime objects and RFC3339 formatted strings for Square API.
        
        Args:
            days_ago (int): Number of days in the past (0 = today, 1 = yesterday)
            tzname (str): Timezone name (default: America/Chicago)
        
        Returns:
            Tuple containing:
                - start: datetime object for start of day (local time)
                - end: datetime object for end of day (local time)
                - start_rfc: RFC3339 formatted start time (UTC)
                - end_rfc: RFC3339 formatted end time (UTC)
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
    start_date: str,
    end_date: str,
    state: str = "COMPLETED",
    test: bool = False
    )-> List[Dict]:
        """
            Fetch orders for a specific location and date range.
            
            Args:
                location_id (str): Square location ID (e.g., "L5WST6KFZBT10")
                start_date (str): Start date in 'YYYY-MM-DD' format
                end_date (str): End date in 'YYYY-MM-DD' format
                state (str): Order state to filter by (default: "COMPLETED")
                test (bool): If True, fetch max 2 pages (200 orders) for testing
            
            Returns:
                List[Dict]: List of order objects from Square API
            
            Raises:
                SquareAPIError: If the API request fails
                ValueError: If date format is invalid
    """
        try:
            local_tz = tz.gettz("America/Chicago")
            
            start_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)
            
            end_dt = dt.datetime.strptime(end_date, "%Y-%m-%d")
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=local_tz)
            
            start_rfc = start_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
            end_rfc = end_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
            
        except ValueError as e:
            raise ValueError(
                f"Invalid date format. Expected 'YYYY-MM-DD', got start_date='{start_date}', end_date='{end_date}'"
            ) from e
        
        logger.info(
            f"Fetching orders for location {location_id} "
            f"from {start_date} to {end_date}"
            f"{' (TEST MODE - max 2 pages/200 orders)' if test else ''}"
        )
        logger.debug(f"RFC3339 range: {start_rfc} to {end_rfc}")
        
        url = f"{self.base_url}/v2/orders/search"
        
        orders = []
        cursor = None
        page_count = 0
        max_pages = 2 if test else None  # Limit to 2 pages in test mode
        
        while True:
            page_count += 1
            
            # Stop if we've reached max pages (test mode)
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
                response = requests.post(url, headers=self.headers, json=body)
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
        days_ago: int = 0,
        state: str = "COMPLETED",
        test: bool = False
    ) -> List[Dict]:
        """
        Fetch orders for a specific location and date.
        
        Convenience wrapper around fetch_orders_by_date_range for single-day queries.
        
        Args:
            location_id (str): Square location ID
            days_ago (int): Number of days in the past (0 = today, 1 = yesterday)
            state (str): Order state to filter by (default: "COMPLETED")
            test (bool): If True, only fetch first 5 orders (for testing)
        
        Returns:
            List[Dict]: List of order objects from Square API
        
        Raises:
            SquareAPIError: If the API request fails
        """
        start_local, end_local, _, _ = self.day_window(days_ago)
        
        start_date = start_local.strftime('%Y-%m-%d')
        end_date = (end_local - dt.timedelta(seconds=1)).strftime('%Y-%m-%d')
        
        return self.fetch_orders_by_date_range(
            location_id=location_id,
            start_date=start_date,
            end_date=end_date,
            state=state,
            test=test
        )
    
    
    def fetch_multiple_locations(
        self,
        location_ids: List[str],
        days_ago: int = 0,
        state: str = "COMPLETED",
        test: bool = False
    ) -> Dict[str, List[Dict]]:
        """
        Fetch orders for multiple locations for the same date.
        
        Args:
            location_ids (List[str]): List of Square location IDs
            days_ago (int): Number of days in the past
            state (str): Order state to filter by
            test (bool): If True, only fetch first 5 orders per location
        
        Returns:
            Dict[str, List[Dict]]: Dictionary mapping location_id to list of orders
        """
        results = {}
        
        for location_id in location_ids:
            try:
                orders = self.fetch_orders_by_date(
                    location_id=location_id,
                    days_ago=days_ago,
                    state=state,
                    test=test
                )
                results[location_id] = orders
                
            except SquareAPIError as e:
                logger.error(f"Failed to fetch orders for location {location_id}: {e}")
                results[location_id] = []
        
        return results
    
    
    def test_connection(self) -> bool:
        """
        Test the Square API connection and authentication.
        
        Makes a simple API call to verify credentials are working.
    
        """
        try:
            url = f"{self.base_url}/v2/locations"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            locations = response.json().get("locations", [])
            logger.info(f"Connection test successful. Found {len(locations)} locations.")
            
            for loc in locations:
                logger.info(f"  - {loc.get('name')} (ID: {loc.get('id')})")
            
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False


# Create singleton instance
square_service = SquareService()


# ============================================================================
# TESTING CODE
# ============================================================================

if __name__ == "__main__":
    """
    Run tests when file is executed directly.
    
    Usage:
        python app/services/square_service.py
    """
    
    # Configure logging for testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    from datetime import timedelta
    
    print("\n" + "#"*60)
    print("# SQUARE SERVICE TEST SUITE")
    print("#"*60)
    
    # Track test results
    results = []
    
    # =================================================================
    # TEST 1: Connection Test
    # =================================================================
    print("\n" + "="*60)
    print("TEST 1: Connection Test")
    print("="*60)
    
    try:
        result = square_service.test_connection()
        if result:
            print("✅ Connection test PASSED")
            results.append(("Connection Test", True))
        else:
            print("❌ Connection test FAILED")
            results.append(("Connection Test", False))
    except Exception as e:
        print(f"❌ Connection test ERROR: {e}")
        results.append(("Connection Test", False))
    
    # =================================================================
    # TEST 2: Fetch Today's Orders (Test Mode - 5 orders)
    # =================================================================
    print("\n" + "="*60)
    print("TEST 2: Fetch Today's Orders (Test Mode - 5 orders)")
    print("="*60)
    
    try:
        # IMPORTANT: Replace with your actual location ID
        location_id = "L5WST6KFZBT10"
        
        orders = square_service.fetch_orders_by_date(
            location_id=location_id,
            days_ago=0,
            test=True
        )
        
        print(f"✅ Fetched {len(orders)} orders")
        
        if len(orders) > 5:
            print(f"❌ ERROR: Expected max 5 orders, got {len(orders)}")
            results.append(("Fetch Today (Test Mode)", False))
        else:
            if orders:
                print(f"\nSample order:")
                print(f"  ID: {orders[0].get('id')}")
                print(f"  Closed at: {orders[0].get('closed_at')}")
                print(f"  Line items: {len(orders[0].get('line_items', []))}")
            print("✅ Test mode PASSED")
            results.append(("Fetch Today (Test Mode)", True))
            
    except SquareAPIError as e:
        print(f"❌ Square API error: {e}")
        results.append(("Fetch Today (Test Mode)", False))
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        results.append(("Fetch Today (Test Mode)", False))
    
    # =================================================================
    # TEST 3: Fetch Yesterday's Orders (Full)
    # =================================================================
    print("\n" + "="*60)
    print("TEST 3: Fetch Yesterday's Orders (Full)")
    print("="*60)
    
    try:
        location_id = "L5WST6KFZBT10"
        
        orders = square_service.fetch_orders_by_date(
            location_id=location_id,
            days_ago=1,
            test=False
        )
        
        print(f"✅ Fetched {len(orders)} orders from yesterday")
        
        if orders:
            print(f"\nFirst order:")
            print(f"  ID: {orders[0].get('id')}")
            print(f"  Closed at: {orders[0].get('closed_at')}")
            
            print(f"\nLast order:")
            print(f"  ID: {orders[-1].get('id')}")
            print(f"  Closed at: {orders[-1].get('closed_at')}")
        
        print("✅ Full fetch PASSED")
        results.append(("Fetch Yesterday (Full)", True))
        
    except SquareAPIError as e:
        print(f"❌ Square API error: {e}")
        results.append(("Fetch Yesterday (Full)", False))
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        results.append(("Fetch Yesterday (Full)", False))
    
    # =================================================================
    # TEST 4: Date Range (Test Mode - 5 orders)
    # =================================================================
    print("\n" + "="*60)
    print("TEST 4: Fetch Date Range (Test Mode - 5 orders)")
    print("="*60)
    
    try:
        location_id = "L5WST6KFZBT10"
        
        end_date = dt.datetime.now().strftime('%Y-%m-%d')
        start_date = (dt.datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        orders = square_service.fetch_orders_by_date_range(
            location_id=location_id,
            start_date=start_date,
            end_date=end_date,
            test=True
        )
        
        print(f"✅ Fetched {len(orders)} orders from {start_date} to {end_date}")
        
        if len(orders) > 5:
            print(f"❌ ERROR: Expected max 5 orders, got {len(orders)}")
            results.append(("Date Range (Test Mode)", False))
        else:
            print("✅ Date range test mode PASSED")
            results.append(("Date Range (Test Mode)", True))
        
    except SquareAPIError as e:
        print(f"❌ Square API error: {e}")
        results.append(("Date Range (Test Mode)", False))
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        results.append(("Date Range (Test Mode)", False))
    
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
        print(f"  Local end: {end}")
        print(f"  UTC start: {start_rfc}")
        print(f"  UTC end: {end_rfc}")
        
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
    # TEST 6: Invalid Date Format Handling
    # =================================================================
    print("\n" + "="*60)
    print("TEST 6: Invalid Date Format Handling")
    print("="*60)
    
    try:
        location_id = "L5WST6KFZBT10"
        
        orders = square_service.fetch_orders_by_date_range(
            location_id=location_id,
            start_date="2024/10/01",
            end_date="2024/10/31",
            test=True
        )
        
        print("❌ ERROR: Should have raised ValueError for invalid date format")
        results.append(("Invalid Date Handling", False))
        
    except ValueError as e:
        print(f"✅ Correctly caught ValueError: {e}")
        print("✅ Error handling PASSED")
        results.append(("Invalid Date Handling", True))
        
    except Exception as e:
        print(f"❌ Unexpected error type: {e}")
        results.append(("Invalid Date Handling", False))
    
    # =================================================================
    # SUMMARY
    # =================================================================
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print("="*60)
    print(f"Results: {passed}/{total} tests passed")
    print("="*60)
    
    exit(0 if passed == total else 1)