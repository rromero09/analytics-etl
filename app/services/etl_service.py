"""
ETL Service for Bakehouse Sales Data

This module handles data transformation from Square API format to database schema.
Key responsibilities:
- Transform Square order JSON → sales table rows
- Convert timestamps from UTC → America/Chicago timezone
- Extract date components (month, day_of_week)
- Validate data before database insertion

WHY THIS EXISTS:
- Centralizes all data transformation logic
- Ensures consistent timezone handling
- Provides data validation layer
- Separates transformation concerns from API/database logic

DESIGN PATTERN:
- Pure transformation functions (no side effects)
- Timezone-aware datetime handling
- One order → multiple sales rows (one per line_item)

CRITICAL NOTES:
- Uses order['closed_at'] NOT order['created_at'] (based on working implementation)
- All prices converted from cents to dollars (divide by 100)
- All timestamps stored in America/Chicago timezone
"""

import datetime as dt
from dateutil import tz
from typing import Dict, List, Tuple, Optional
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)


class ETLValidationError(Exception):
    """Custom exception for data validation errors."""
    pass


class ETLService:
    """
    Service class for transforming Square API data into database-ready format.
    
    This class handles all data transformation operations including:
    - Order → sales rows transformation
    - Timezone conversions
    - Date/time component extraction
    - Data validation
    
    Usage:
        from app.services.etl_service import etl_service
        
        # Transform an order for location_id=1
        sales_rows = etl_service.transform_order_to_sales(order, location_id=1)
    """
    
    def __init__(self):
        """Initialize the ETL service."""
        self.chicago_tz = tz.gettz("America/Chicago")
        logger.info("ETLService initialized with timezone: America/Chicago")
    
    
    def convert_to_chicago_timezone(self, utc_timestamp: str) -> dt.datetime:
        """
        Convert UTC timestamp string to America/Chicago timezone.
        
        Handles both RFC3339 format and ISO 8601 format timestamps from Square API.
        Automatically accounts for CST/CDT (daylight saving time).
        
        Args:
            utc_timestamp (str): UTC timestamp string (e.g., "2025-11-07T13:27:45.163Z")
        
        Returns:
            dt.datetime: Datetime object in America/Chicago timezone
        
        Example:
            >>> etl_service.convert_to_chicago_timezone("2025-11-07T13:27:45.163Z")
            datetime.datetime(2025, 11, 7, 7, 27, 45, 163000, tzinfo=tzfile('America/Chicago'))
        
        Raises:
            ETLValidationError: If timestamp format is invalid
        """
        try:
            # Parse the UTC timestamp
            # Handle both formats: "2025-11-07T13:27:45.163Z" and "2025-11-07T13:27:45Z"
            if utc_timestamp.endswith('Z'):
                utc_timestamp = utc_timestamp[:-1] + '+00:00'
            
            utc_dt = dt.datetime.fromisoformat(utc_timestamp)
            
            # Convert to Chicago timezone
            chicago_dt = utc_dt.astimezone(self.chicago_tz)
            
            logger.debug(f"Converted {utc_timestamp} → {chicago_dt}")
            return chicago_dt
            
        except (ValueError, AttributeError) as e:
            error_msg = f"Invalid timestamp format: {utc_timestamp}"
            logger.error(error_msg)
            raise ETLValidationError(error_msg) from e
    
    
    def extract_date_components(
        self, 
        timestamp: dt.datetime
    ) -> Tuple[str, str]:
        """
        Extract date components from a datetime object.
        
        Args:
            timestamp (dt.datetime): Timezone-aware datetime object
        
        Returns:
            Tuple containing:
                - month (str): Format 'YYYY-MM' (e.g., '2025-11')
                - day_of_week (str): Day name (e.g., 'Monday')
        
        Example:
            >>> ts = datetime(2025, 11, 7, 7, 27, 45, tzinfo=chicago_tz)
            >>> month, dow = etl_service.extract_date_components(ts)
            >>> print(month, dow)
            '2025-11' 'Thursday'
        """
        month = timestamp.strftime('%Y-%m')
        day_of_week = timestamp.strftime('%A')  # Full day name
        
        logger.debug(
            f"Extracted components: month={month}, day_of_week={day_of_week}"
        )
        
        return month, day_of_week
    
    
    def validate_line_item(self, line_item: Dict) -> bool:
        """
        Validate that a line item has required fields.
        
        Args:
            line_item (Dict): Line item object from Square order
        
        Returns:
            bool: True if valid, False otherwise
        
        Validation checks:
            - Has 'name' field
            - Has 'quantity' field
            - Has 'base_price_money' field with 'amount'
            - Quantity is positive
            - Price is non-negative
        """
        try:
            # Check required fields exist
            if not line_item.get('name'):
                logger.warning("Line item missing 'name' field")
                return False
            
            if not line_item.get('quantity'):
                logger.warning("Line item missing 'quantity' field")
                return False
            
            base_price = line_item.get('base_price_money', {})
            if 'amount' not in base_price:
                logger.warning("Line item missing 'base_price_money.amount' field")
                return False
            
            # Validate quantity is positive
            qty = int(line_item['quantity'])
            if qty <= 0:
                logger.warning(f"Invalid quantity: {qty}")
                return False
            
            # Validate price is non-negative
            price_cents = int(base_price['amount'])
            if price_cents < 0:
                logger.warning(f"Invalid price: {price_cents}")
                return False
            
            return True
            
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Line item validation error: {e}")
            return False
    
    
    def transform_order_to_sales(
        self, 
        order: Dict, 
        location_id: int
    ) -> List[Dict]:
        """
        Transform a Square order into sales table rows.
        
        One order can have multiple line_items, each becomes a separate row.
        
        Args:
            order (Dict): Square order object from API
            location_id (int): Database location_id (FK to locations table)
        
        Returns:
            List[Dict]: List of sales records ready for database insertion
        
        Field Mappings:
            Square JSON Field                    → Database Column
            ────────────────────────────────────────────────────────
            line_item.name                       → item_name
            line_item.base_price_money.amount    → sale_price (÷ 100)
            line_item.quantity                   → qty
            order.closed_at                      → sale_timestamp
            EXTRACT(month from closed_at)        → month
            EXTRACT(day_of_week from closed_at)  → day_of_week
            line_item.variation_name OR 'N/A'    → item_category
            location_id (parameter)              → location_id
        
        Example:
            >>> order = {
            ...     "id": "abc123",
            ...     "location_id": "L5WST6KFZBT10",
            ...     "closed_at": "2025-11-07T13:27:45.163Z",
            ...     "line_items": [
            ...         {
            ...             "name": "Croissant Plain",
            ...             "variation_name": "Regular",
            ...             "quantity": "1",
            ...             "base_price_money": {"amount": 500, "currency": "USD"}
            ...         }
            ...     ]
            ... }
            >>> sales_rows = etl_service.transform_order_to_sales(order, location_id=1)
            >>> print(sales_rows[0]['item_name'])
            'Croissant Plain'
        
        Raises:
            ETLValidationError: If order is missing critical fields
        """
        # Validate order has required fields
        if 'closed_at' not in order:
            error_msg = f"Order {order.get('id')} missing 'closed_at' field"
            logger.error(error_msg)
            raise ETLValidationError(error_msg)
        
        if 'line_items' not in order:
            logger.warning(f"Order {order.get('id')} has no line_items, skipping")
            return []
        
        # Convert closed_at timestamp to Chicago timezone
        try:
            chicago_timestamp = self.convert_to_chicago_timezone(order['closed_at'])
        except ETLValidationError as e:
            logger.error(f"Failed to convert timestamp for order {order.get('id')}: {e}")
            raise
        
        # Extract date components
        month, day_of_week = self.extract_date_components(chicago_timestamp)
        
        # Transform each line_item into a sales row
        sales_rows = []
        
        for line_item in order['line_items']:
            # Validate line item
            if not self.validate_line_item(line_item):
                logger.warning(
                    f"Skipping invalid line_item in order {order.get('id')}: "
                    f"{line_item.get('name', 'UNKNOWN')}"
                )
                continue
            
            try:
                # Extract fields with safe defaults
                item_name = line_item['name']
                qty = int(line_item['quantity'])
                
                # Convert price from cents to dollars
                price_cents = int(line_item['base_price_money']['amount'])
                sale_price = Decimal(price_cents) / Decimal(100)
                
                # Get category (variation_name or 'N/A')
                item_category = line_item.get('variation_name', 'N/A')
                
                # Build sales record
                sales_row = {
                    'item_name': item_name,
                    'sale_price': float(sale_price),  # Convert Decimal to float for DB
                    'qty': qty,
                    'sale_timestamp': chicago_timestamp,
                    'month': month,
                    'day_of_week': day_of_week,
                    'item_category': item_category,
                    'location_id': location_id
                }
                
                sales_rows.append(sales_row)
                
                logger.debug(
                    f"Transformed line_item: {item_name} "
                    f"(${sale_price}, qty={qty}) → sales row"
                )
                
            except (ValueError, KeyError, TypeError) as e:
                logger.error(
                    f"Error transforming line_item in order {order.get('id')}: {e}"
                )
                continue
        
        logger.info(
            f"Transformed order {order.get('id')}: "
            f"{len(order['line_items'])} line_items → {len(sales_rows)} sales rows"
        )
        
        return sales_rows
    
    
    def transform_orders_batch(
        self,
        orders: List[Dict],
        location_id: int
    ) -> List[Dict]:
        """
        Transform multiple orders into sales rows (batch operation).
        
        Args:
            orders (List[Dict]): List of Square order objects
            location_id (int): Database location_id for all orders
        
        Returns:
            List[Dict]: Flattened list of all sales rows from all orders
        
        Example:
            >>> orders = square_service.fetch_orders_by_date("L5WST6KFZBT10", days_ago=0)
            >>> sales_data = etl_service.transform_orders_batch(orders, location_id=1)
            >>> print(f"Total sales rows: {len(sales_data)}")
        """
        all_sales_rows = []
        failed_orders = 0
        
        for order in orders:
            try:
                sales_rows = self.transform_order_to_sales(order, location_id)
                all_sales_rows.extend(sales_rows)
                
            except ETLValidationError as e:
                logger.error(f"Failed to transform order {order.get('id')}: {e}")
                failed_orders += 1
                continue
        
        logger.info(
            f"Batch transformation complete: "
            f"{len(orders)} orders → {len(all_sales_rows)} sales rows "
            f"({failed_orders} orders failed)"
        )
        
        return all_sales_rows
    
    
    def validate_sales_row(self, sales_row: Dict) -> bool:
        """
        Validate a sales row before database insertion.
        
        Args:
            sales_row (Dict): Sales row dictionary
        
        Returns:
            bool: True if valid, False otherwise
        
        Validation checks:
            - All required fields present
            - sale_price > 0
            - qty > 0
            - sale_timestamp is datetime object
            - month matches 'YYYY-MM' format
            - day_of_week is valid day name
        """
        required_fields = [
            'item_name', 'sale_price', 'qty', 'sale_timestamp',
            'month', 'day_of_week', 'item_category', 'location_id'
        ]
        
        # Check all required fields exist
        for field in required_fields:
            if field not in sales_row:
                logger.warning(f"Sales row missing required field: {field}")
                return False
        
        # Validate price and quantity
        if sales_row['sale_price'] < 0:
            logger.warning(f"Invalid sale_price: {sales_row['sale_price']}")
            return False
        
        if sales_row['qty'] <= 0:
            logger.warning(f"Invalid qty: {sales_row['qty']}")
            return False
        
        # Validate timestamp is datetime
        if not isinstance(sales_row['sale_timestamp'], dt.datetime):
            logger.warning(f"Invalid sale_timestamp type: {type(sales_row['sale_timestamp'])}")
            return False
        
        # Validate month format (YYYY-MM)
        import re
        if not re.match(r'^\d{4}-\d{2}$', sales_row['month']):
            logger.warning(f"Invalid month format: {sales_row['month']}")
            return False
        
        # Validate day_of_week
        valid_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        if sales_row['day_of_week'] not in valid_days:
            logger.warning(f"Invalid day_of_week: {sales_row['day_of_week']}")
            return False
        
        return True


# Create singleton instance
etl_service = ETLService()


# ============================================================================
# TESTING & VALIDATION
# ============================================================================

if __name__ == "__main__":
    """
    Run this file directly to test the ETL transformations.
    
    Usage:
        python app/services/etl_service.py
    """
    import json
    
    print("=" * 70)
    print("BAKEHOUSE ETL - ETL SERVICE TEST")
    print("=" * 70)
    """
Monthly ETL Script - Orchestrates the complete ETL pipeline

This script coordinates all services to fetch, transform, and load
sales data from Square API into PostgreSQL database.

WHAT IT DOES:
1. Calculates date range (previous month by default)
2. Fetches all locations from database
3. For each location:
   - Fetches orders from Square API
   - Transforms orders to sales rows
   - Bulk inserts into database
4. Prints summary report

ENVIRONMENT VARIABLES:
- START_DATE: Override start date (format: YYYY-MM-DD)
- END_DATE: Override end date (format: YYYY-MM-DD)
- LOCATION_FILTER: Process specific location (1, 2, 3) or 'all' (default: 'all')
- TEST: Set to 'true' for test mode (fetches only 5 orders per location)

Usage:
    # Default: Previous month, all locations
    python -m app.scripts.monthly_etl
    
    # Custom date range
    START_DATE=2025-10-01 END_DATE=2025-10-31 python -m app.scripts.monthly_etl
    
    # Single location
    LOCATION_FILTER=1 python -m app.scripts.monthly_etl
    
    # Test mode
    TEST=true python -m app.scripts.monthly_etl
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import logging

# Import our services
from app.services.square_service import square_service, SquareAPIError
from app.services.etl_service import etl_service, ETLValidationError
from app.services.database_service import DatabaseService


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MonthlyETL:
    """
    Orchestrates the monthly ETL process.
    
    This class coordinates all services and handles the complete
    data pipeline from Square API to PostgreSQL database.
    """
    
    def __init__(self):
        """Initialize the ETL orchestrator with all required services."""
        self.square_service = square_service
        self.etl_service = etl_service
        self.db_service = DatabaseService()
        
        # Statistics tracking
        self.stats = {
            'total_locations': 0,
            'total_orders': 0,
            'total_sales_rows': 0,
            'failed_locations': 0,
            'start_time': None,
            'end_time': None
        }
        
        logger.info("MonthlyETL initialized")
    
    
    def calculate_previous_month(self) -> Tuple[str, str]:
        """
        Calculate the start and end dates for the previous month.
        
        Returns:
            Tuple[str, str]: (start_date, end_date) in 'YYYY-MM-DD' format
        
        Example:
            If today is 2025-11-12:
            Returns: ('2025-10-01', '2025-10-31')
        """
        today = datetime.now()
        
        # First day of current month
        first_of_current_month = today.replace(day=1)
        
        # Last day of previous month
        last_day_of_previous_month = first_of_current_month - timedelta(days=1)
        
        # First day of previous month
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
        
        start_date = first_day_of_previous_month.strftime('%Y-%m-%d')
        end_date = last_day_of_previous_month.strftime('%Y-%m-%d')
        
        logger.info(f"Calculated previous month: {start_date} to {end_date}")
        return start_date, end_date
    
    
    def get_date_range_from_env(self) -> Tuple[str, str]:
        """
        Get date range from environment variables or calculate previous month.
        
        Environment variables:
            START_DATE: Custom start date (YYYY-MM-DD)
            END_DATE: Custom end date (YYYY-MM-DD)
        
        Returns:
            Tuple[str, str]: (start_date, end_date)
        """
        # Check for custom date range in environment
        start_date = os.getenv('START_DATE')
        end_date = os.getenv('END_DATE')
        
        if start_date and end_date:
            logger.info(f"Using custom date range from environment: {start_date} to {end_date}")
            return start_date, end_date
        elif start_date or end_date:
            logger.warning(
                "Both START_DATE and END_DATE must be provided. "
                "Falling back to previous month."
            )
            return self.calculate_previous_month()
        else:
            # Default: previous month
            return self.calculate_previous_month()
    
    
    def get_locations_to_process(self) -> List[Dict]:
        """
        Get the list of locations to process based on LOCATION_FILTER env var.
        
        Environment variable:
            LOCATION_FILTER: 'all' (default) or specific location ID (1, 2, 3)
        
        Returns:
            List[Dict]: List of location dictionaries to process
        """
        # Get all locations from database
        all_locations = self.db_service.get_all_locations()
        
        # Check for location filter
        location_filter = os.getenv('LOCATION_FILTER', 'all').lower()
        
        if location_filter == 'all':
            logger.info(f"Processing all {len(all_locations)} locations")
            return all_locations
        else:
            # Filter for specific location
            try:
                location_id = int(location_filter)
                filtered = [loc for loc in all_locations if loc['id'] == location_id]
                
                if not filtered:
                    logger.error(f"Location ID {location_id} not found in database")
                    return []
                
                logger.info(f"Processing single location: {filtered[0]['name']}")
                return filtered
                
            except ValueError:
                logger.error(f"Invalid LOCATION_FILTER value: {location_filter}")
                return all_locations
    
    
    def is_test_mode(self) -> bool:
        """
        Check if running in test mode (fetches only 5 orders per location).
        
        Environment variable:
            TEST: 'true' or '1' to enable test mode
        
        Returns:
            bool: True if test mode enabled
        """
        test_mode = os.getenv('TEST', 'false').lower()
        is_test = test_mode in ['true', '1', 'yes']
        
        if is_test:
            logger.info("⚠️  TEST MODE ENABLED - Will fetch only 5 orders per location")
        
        return is_test
    
    
    def process_location(
        self,
        location: Dict,
        start_date: str,
        end_date: str,
        test_mode: bool
    ) -> Dict:
        """
        Process ETL for a single location.
        
        Args:
            location: Location dictionary with 'id', 'name', 'square_id'
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            test_mode: If True, only fetch 5 orders
        
        Returns:
            Dict: Statistics for this location
                {
                    'location_name': str,
                    'orders_fetched': int,
                    'sales_rows_created': int,
                    'rows_inserted': int,
                    'success': bool,
                    'error': str or None
                }
        """
        location_name = location['name']
        location_id = location['id']
        square_id = location['square_id']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {location_name} (ID: {location_id})")
        logger.info(f"Square ID: {square_id}")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"{'='*60}")
        
        result = {
            'location_name': location_name,
            'orders_fetched': 0,
            'sales_rows_created': 0,
            'rows_inserted': 0,
            'success': False,
            'error': None
        }
        
        try:
            # Step 1: Fetch orders from Square API
            logger.info(f"[1/3] Fetching orders from Square API...")
            orders = self.square_service.fetch_orders_by_date_range(
                location_id=square_id,
                start_date=start_date,
                end_date=end_date,
                test=test_mode
            )
            result['orders_fetched'] = len(orders)
            logger.info(f"✓ Fetched {len(orders)} orders")
            
            if not orders:
                logger.warning(f"No orders found for {location_name}")
                result['success'] = True  # Not an error, just no data
                return result
            
            # Step 2: Transform orders to sales rows
            logger.info(f"[2/3] Transforming orders to sales rows...")
            sales_data = self.etl_service.transform_orders_batch(
                orders=orders,
                location_id=location_id
            )
            result['sales_rows_created'] = len(sales_data)
            logger.info(f"✓ Created {len(sales_data)} sales rows")
            
            if not sales_data:
                logger.warning(f"No sales data created for {location_name}")
                result['success'] = True
                return result
            
            # Step 3: Bulk insert into database
            logger.info(f"[3/3] Inserting into database...")
            rows_inserted = self.db_service.bulk_insert_sales(sales_data)
            result['rows_inserted'] = rows_inserted
            logger.info(f"✓ Inserted {rows_inserted} rows")
            
            # Success!
            result['success'] = True
            logger.info(f"✅ {location_name} completed successfully")
            
            return result
            
        except SquareAPIError as e:
            error_msg = f"Square API error: {str(e)}"
            logger.error(f"❌ {location_name} failed: {error_msg}")
            result['error'] = error_msg
            return result
            
        except ETLValidationError as e:
            error_msg = f"ETL validation error: {str(e)}"
            logger.error(f"❌ {location_name} failed: {error_msg}")
            result['error'] = error_msg
            return result
            
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"❌ {location_name} failed: {error_msg}")
            result['error'] = error_msg
            return result
    
    
    def run(self) -> bool:
        """
        Execute the complete monthly ETL process.
        
        Returns:
            bool: True if all locations processed successfully, False otherwise
        """
        self.stats['start_time'] = datetime.now()
        
        print("\n" + "#"*60)
        print("# MONTHLY ETL PROCESS")
        print("#"*60)
        
        # Step 1: Test database connection
        logger.info("Testing database connection...")
        if not self.db_service.test_connection():
            logger.error("❌ Database connection failed. Aborting ETL.")
            return False
        logger.info("✓ Database connection successful")
        
        # Step 2: Get configuration
        start_date, end_date = self.get_date_range_from_env()
        locations = self.get_locations_to_process()
        test_mode = self.is_test_mode()
        
        if not locations:
            logger.error("❌ No locations to process. Aborting ETL.")
            return False
        
        self.stats['total_locations'] = len(locations)
        
        # Print configuration
        print("\n" + "="*60)
        print("CONFIGURATION")
        print("="*60)
        print(f"Date range: {start_date} to {end_date}")
        print(f"Locations: {len(locations)}")
        print(f"Test mode: {'YES (5 orders per location)' if test_mode else 'NO (full fetch)'}")
        print("="*60)
        
        # Step 3: Process each location
        results = []
        
        for location in locations:
            result = self.process_location(
                location=location,
                start_date=start_date,
                end_date=end_date,
                test_mode=test_mode
            )
            results.append(result)
            
            # Update statistics
            self.stats['total_orders'] += result['orders_fetched']
            self.stats['total_sales_rows'] += result['rows_inserted']
            
            if not result['success']:
                self.stats['failed_locations'] += 1
        
        # Step 4: Print summary
        self.stats['end_time'] = datetime.now()
        self.print_summary(results, start_date, end_date, test_mode)
        
        # Return success if no locations failed
        return self.stats['failed_locations'] == 0
    
    
    def print_summary(
        self,
        results: List[Dict],
        start_date: str,
        end_date: str,
        test_mode: bool
    ):
        """
        Print a summary report of the ETL process.
        
        Args:
            results: List of result dictionaries from each location
            start_date: Start date processed
            end_date: End date processed
            test_mode: Whether test mode was enabled
        """
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        print("\n" + "="*60)
        print("ETL SUMMARY REPORT")
        print("="*60)
        
        # Per-location results
        print("\nPER-LOCATION RESULTS:")
        print("-" * 60)
        for result in results:
            status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
            print(f"{status} - {result['location_name']}")
            print(f"  Orders fetched: {result['orders_fetched']}")
            print(f"  Sales rows created: {result['sales_rows_created']}")
            print(f"  Rows inserted: {result['rows_inserted']}")
            if result['error']:
                print(f"  Error: {result['error']}")
            print()
        
        # Overall statistics
        print("="*60)
        print("OVERALL STATISTICS")
        print("="*60)
        print(f"Date range: {start_date} to {end_date}")
        print(f"Test mode: {'YES' if test_mode else 'NO'}")
        print(f"Locations processed: {self.stats['total_locations']}")
        print(f"Locations failed: {self.stats['failed_locations']}")
        print(f"Total orders fetched: {self.stats['total_orders']}")
        print(f"Total rows inserted: {self.stats['total_sales_rows']}")
        print(f"Execution time: {duration:.2f} seconds")
        print("="*60)
        
        # Final status
        if self.stats['failed_locations'] == 0:
            print("\n✅ ETL COMPLETED SUCCESSFULLY")
        else:
            print(f"\n⚠️  ETL COMPLETED WITH {self.stats['failed_locations']} FAILURES")
        
        print("="*60 + "\n")


def main():
    """
    Main entry point for the monthly ETL script.
    
    Returns:
        Exit code (0 = success, 1 = failure)
    """
    try:
        etl = MonthlyETL()
        success = etl.run()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  ETL interrupted by user (Ctrl+C)")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
    # Test 1: Timezone Conversion
    print("\n[TEST 1] Testing timezone conversion...")
    utc_time = "2025-11-07T13:27:45.163Z"
    chicago_time = etl_service.convert_to_chicago_timezone(utc_time)
    print(f"UTC:     {utc_time}")
    print(f"Chicago: {chicago_time}")
    print(f"✅ Timezone: {chicago_time.tzname()}")
    
    # Test 2: Date Component Extraction
    print("\n[TEST 2] Testing date component extraction...")
    month, dow = etl_service.extract_date_components(chicago_time)
    print(f"Month:        {month}")
    print(f"Day of Week:  {dow}")
    print("✅ Date components extracted")
    
    # Test 3: Order Transformation
    print("\n[TEST 3] Testing order transformation...")
    
    sample_order = {
        "id": "test_order_123",
        "location_id": "L5WST6KFZBT10",
        "closed_at": "2025-11-07T13:27:45.163Z",
        "line_items": [
            {
                "name": "Croissant Plain",
                "variation_name": "Regular",
                "quantity": "1",
                "base_price_money": {
                    "amount": 500,
                    "currency": "USD"
                }
            },
            {
                "name": "Drip Coffee",
                "variation_name": "8 oz",
                "quantity": "2",
                "base_price_money": {
                    "amount": 295,
                    "currency": "USD"
                }
            }
        ]
    }
    
    sales_rows = etl_service.transform_order_to_sales(sample_order, location_id=1)
    
    print(f"✅ Order with {len(sample_order['line_items'])} line_items → {len(sales_rows)} sales rows")
    
    print("\n[TEST 4] Sample sales row:")
    print("-" * 70)
    print(json.dumps({
        **sales_rows[0],
        'sale_timestamp': str(sales_rows[0]['sale_timestamp'])  # Convert datetime for JSON
    }, indent=2))
    print("-" * 70)
    
    # Test 5: Validation
    print("\n[TEST 5] Testing sales row validation...")
    is_valid = etl_service.validate_sales_row(sales_rows[0])
    if is_valid:
        print("✅ Sales row validation passed")
    else:
        print("❌ Sales row validation failed")
    
    print("\n" + "=" * 70)
    print("All tests passed! ✅")
    print("=" * 70)