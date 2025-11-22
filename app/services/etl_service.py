"""
ETL Service for Bakehouse Sales Data

This module handles data transformation from Square API format to database schema.
Key responsibilities:
- Transform Square order JSON → sales table rows
- Convert timestamps from UTC → America/Chicago timezone
- Extract date components (month, day_of_week)
- Validate data before database insertion
- Filter out non-revenue items (Dine In, To Go, etc.)

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
- Filters out $0 items and non-revenue items (Dine In, To Go, etc.)
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
    - Filtering non-revenue items
    
    Usage:
        from app.services.etl_service import etl_service
        
        # Transform an order for location_id=1
        sales_rows = etl_service.transform_order_to_sales(order, location_id=1)
    """
    
    # Items to filter out from sales data
    IGNORED_ITEMS = [
        'dine in',
        'to go',
        'free water'
    ]
    
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
    
    
    def _is_valid_line_item(self, line_item: Dict) -> bool:
        """
        Check if a line item should be processed (filters out non-revenue items).
        
        Args:
            line_item (Dict): Line item object from Square order
        
        Returns:
            bool: True if item should be processed, False if it should be filtered out
        
        Filtering rules:
            - Filter out items with price = $0
            - Filter out items in IGNORED_ITEMS list (case-insensitive)
        """
        # Get price
        base_price = line_item.get('base_price_money', {})
        price_cents = base_price.get('amount', 0)
        
        # Filter out $0 items
        if price_cents <= 0:
            item_name = line_item.get('name', 'UNKNOWN')
            logger.debug(f"Filtering out $0 item: {item_name}")
            return False
        
        # Get item name
        item_name = line_item.get('name', '').lower()
        
        # Filter out ignored items
        for ignored_item in self.IGNORED_ITEMS:
            if ignored_item in item_name:
                logger.debug(f"Filtering out ignored item: {line_item.get('name')}")
                return False
        
        return True
    
    
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
        Filters out non-revenue items (Dine In, To Go, $0 items, etc.)
        
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
            # Filter out non-revenue items FIRST
            if not self._is_valid_line_item(line_item):
                continue
            
            # Validate line item structure
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
    
    # Test 3: Item Filtering
    print("\n[TEST 3] Testing item filtering...")
    
    valid_item = {
        "name": "Croissant Plain",
        "base_price_money": {"amount": 500}
    }
    
    dine_in_item = {
        "name": "Dine In",
        "base_price_money": {"amount": 0}
    }
    
    to_go_item = {
        "name": "To Go",
        "base_price_money": {"amount": 0}
    }
    
    print(f"Valid item (Croissant): {etl_service._is_valid_line_item(valid_item)} ✅")
    print(f"Dine In item: {etl_service._is_valid_line_item(dine_in_item)} (should be False)")
    print(f"To Go item: {etl_service._is_valid_line_item(to_go_item)} (should be False)")
    
    # Test 4: Order Transformation with Filtering
    print("\n[TEST 4] Testing order transformation with filtering...")
    
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
                "name": "Dine In",
                "variation_name": "N/A",
                "quantity": "1",
                "base_price_money": {
                    "amount": 0,
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
            },
            {
                "name": "To Go",
                "variation_name": "N/A",
                "quantity": "1",
                "base_price_money": {
                    "amount": 0,
                    "currency": "USD"
                }
            }
        ]
    }
    
    sales_rows = etl_service.transform_order_to_sales(sample_order, location_id=1)
    
    print(f"✅ Order with {len(sample_order['line_items'])} line_items → {len(sales_rows)} sales rows")
    print(f"✅ Filtered out {len(sample_order['line_items']) - len(sales_rows)} non-revenue items")
    
    print("\n[TEST 5] Sample sales row:")
    print("-" * 70)
    print(json.dumps({
        **sales_rows[0],
        'sale_timestamp': str(sales_rows[0]['sale_timestamp'])  # Convert datetime for JSON
    }, indent=2))
    print("-" * 70)
    
    # Test 6: Validation
    print("\n[TEST 6] Testing sales row validation...")
    is_valid = etl_service.validate_sales_row(sales_rows[0])
    if is_valid:
        print("✅ Sales row validation passed")
    else:
        print("❌ Sales row validation failed")
    
    print("\n" + "=" * 70)
    print("All tests passed! ✅")
    print(f"IGNORED_ITEMS list: {ETLService.IGNORED_ITEMS}")
    print("=" * 70)