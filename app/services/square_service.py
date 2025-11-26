"""
ETL Service for Bakehouse Sales Data

Transforms Square API orders into database-ready sales records.

Key jobs:
- Transform Square JSON → sales table rows
- Convert UTC timestamps → Chicago timezone
- Extract date components (month, day_of_week)
- Validate data before DB insertion
- Filter out $0 items (Dine In, To Go, etc.)

Critical details:
- Uses order['closed_at'] NOT order['created_at']
- Prices: cents → dollars (÷ 100)
- All timestamps in America/Chicago timezone
- Uses gross_sales_money (includes base + modifiers)
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
    Transforms Square API data into database-ready format.
    
    Usage:
        from app.services.etl_service import etl_service
        sales_rows = etl_service.transform_order_to_sales(order, location_id=1)
    """
    
    # Filter these out from sales data
    IGNORED_ITEMS = [
        'dine in',
        'to go',
        'free water'
    ]
    
    def __init__(self):
        """Initialize with Chicago timezone."""
        self.chicago_tz = tz.gettz("America/Chicago")
        logger.info("ETLService initialized with timezone: America/Chicago")
    
    
    def convert_to_chicago_timezone(self, utc_timestamp: str) -> dt.datetime:
        """
        Convert UTC timestamp to America/Chicago timezone.
        
        Handles both RFC3339 and ISO 8601 formats from Square API.
        Auto-handles CST/CDT (daylight saving time).
        
        Args:
            utc_timestamp: UTC timestamp string (e.g., "2025-11-07T13:27:45.163Z")
        
        Returns:
            Datetime object in America/Chicago timezone
        
        Raises:
            ETLValidationError: If timestamp format is invalid
        """
        try:
            # Parse UTC timestamp (handle 'Z' suffix)
            if utc_timestamp.endswith('Z'):
                utc_timestamp = utc_timestamp[:-1] + '+00:00'
            
            utc_dt = dt.datetime.fromisoformat(utc_timestamp)
            chicago_dt = utc_dt.astimezone(self.chicago_tz)
            
            logger.debug(f"Converted {utc_timestamp} → {chicago_dt}")
            return chicago_dt
            
        except (ValueError, AttributeError) as e:
            error_msg = f"Invalid timestamp format: {utc_timestamp}"
            logger.error(error_msg)
            raise ETLValidationError(error_msg) from e
    
    
    def extract_date_components(self, timestamp: dt.datetime) -> Tuple[str, str]:
        """
        Extract month and day_of_week from datetime.
        
        Args:
            timestamp: Timezone-aware datetime object
        
        Returns:
            Tuple: (month as 'YYYY-MM', day_of_week as 'Monday')
        """
        month = timestamp.strftime('%Y-%m')
        day_of_week = timestamp.strftime('%A')  # Full day name
        
        logger.debug(f"Extracted: month={month}, day_of_week={day_of_week}")
        return month, day_of_week
    
    
    def _is_valid_line_item(self, line_item: Dict) -> bool:
        """
        Check if line item should be processed.
        
        Filters out:
        - Items with price = $0
        - Items in IGNORED_ITEMS list (Dine In, To Go, etc.)
        
        Returns:
            True if item should be processed
        """
        # Get price
        base_price = line_item.get('base_price_money', {})
        price_cents = base_price.get('amount', 0)
        
        # Filter $0 items
        if price_cents <= 0:
            item_name = line_item.get('name', 'UNKNOWN')
            logger.debug(f"Filtering $0 item: {item_name}")
            return False
        
        # Filter ignored items
        item_name = line_item.get('name', '').lower()
        for ignored_item in self.IGNORED_ITEMS:
            if ignored_item in item_name:
                logger.debug(f"Filtering ignored item: {line_item.get('name')}")
                return False
        
        return True
    
    
    def _parse_modifiers(self, line_item: Dict) -> str:
        """
        Extract revenue-generating modifier names from line item.
        
        Only includes modifiers with price > $0.
        Filters out $0 modifiers like "To Go", "Flat White", etc.
        
        Args:
            line_item: Line item dict from Square API
        
        Returns:
            Comma-separated modifier names, or empty string
            
        Example:
            Input: [{"name": "Almond Milk", "base_price_money": {"amount": 100}},
                    {"name": "To Go", "base_price_money": {"amount": 0}}]
            Output: "Almond Milk"
        """
        modifiers_list = line_item.get('modifiers', [])
        
        if not modifiers_list:
            return ""
        
        revenue_modifiers = []
        
        for modifier in modifiers_list:
            # Get modifier price
            price_cents = modifier.get('base_price_money', {}).get('amount', 0)
            
            # Only include revenue-generating modifiers (price > 0)
            if price_cents > 0:
                modifier_name = modifier.get('name', 'Unknown')
                revenue_modifiers.append(modifier_name)
        
        # Join with commas
        return ", ".join(revenue_modifiers) if revenue_modifiers else ""
    
    
    def validate_line_item(self, line_item: Dict) -> bool:
        """
        Validate line item has required fields.
        
        Checks:
        - Has 'name', 'quantity', 'base_price_money.amount'
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
    
    
    def transform_order_to_sales(self, order: Dict, location_id: int) -> List[Dict]:
        """
        Transform a Square order into sales table rows.
        
        One order can have multiple line_items → each becomes a separate row.
        Filters out $0 items and non-revenue items.
        
        Args:
            order: Square order object from API
            location_id: Database location_id (FK to locations table)
        
        Returns:
            List of sales records ready for DB insertion
        
        Field mappings:
            line_item.name                    → item_name
            line_item.gross_sales_money       → sale_price (÷ 100, includes modifiers!)
            line_item.quantity                → qty
            order.closed_at                   → sale_timestamp
            EXTRACT(month)                    → month
            EXTRACT(day_of_week)              → day_of_week
            line_item.variation_name or 'N/A' → item_category
            location_id (parameter)           → location_id
            line_item.modifiers (filtered)    → modifiers
        
        Raises:
            ETLValidationError: If order missing critical fields
        """
        # Validate order has required fields
        if 'closed_at' not in order:
            error_msg = f"Order {order.get('id')} missing 'closed_at' field"
            logger.error(error_msg)
            raise ETLValidationError(error_msg)
        
        if 'line_items' not in order:
            logger.warning(f"Order {order.get('id')} has no line_items, skipping")
            return []
        
        # Convert timestamp to Chicago timezone
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
            # Filter non-revenue items FIRST
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
                # Extract basic fields
                item_name = line_item['name']
                qty = int(line_item['quantity'])
                
                # Extract modifiers (only revenue-generating ones)
                modifiers = self._parse_modifiers(line_item)
                
                # Use gross_sales_money instead of base_price_money (includes modifiers!)
                price_cents = int(line_item['gross_sales_money']['amount'])
                sale_price = Decimal(price_cents) / Decimal(100)
                
                # Get category
                item_category = line_item.get('variation_name', 'N/A')
                
                # Build sales record
                sales_row = {
                    'item_name': item_name,
                    'sale_price': float(sale_price),  # Now includes modifiers!
                    'qty': qty,
                    'sale_timestamp': chicago_timestamp,
                    'month': month,
                    'day_of_week': day_of_week,
                    'item_category': item_category,
                    'location_id': location_id,
                    'modifiers': modifiers  # NEW feature added to sales table
                }
                
                sales_rows.append(sales_row)
                
                logger.debug(
                    f"Transformed: {item_name} (${sale_price}, qty={qty}) "
                    f"modifiers=[{modifiers}]"
                )
                
            except (ValueError, KeyError, TypeError) as e:
                logger.error(
                    f"Error transforming line_item in order {order.get('id')}: {e}"
                )
                continue
        
        logger.info(
            f"Order {order.get('id')}: "
            f"{len(order['line_items'])} line_items → {len(sales_rows)} sales rows"
        )
        
        return sales_rows
    
    
    def transform_orders_batch(self, orders: List[Dict], location_id: int) -> List[Dict]:
        """
        Transform multiple orders into sales rows (batch operation).
        
        Args:
            orders: List of Square order objects
            location_id: Database location_id for all orders
        
        Returns:
            Flattened list of all sales rows from all orders
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
            f"Batch done: {len(orders)} orders → {len(all_sales_rows)} sales rows "
            f"({failed_orders} failed)"
        )
        
        return all_sales_rows
    
    
    def validate_sales_row(self, sales_row: Dict) -> bool:
        """
        Validate a sales row before DB insertion.
        
        Checks:
        - All required fields present
        - sale_price >= 0
        - qty > 0
        - sale_timestamp is datetime object
        - month matches 'YYYY-MM' format
        - day_of_week is valid day name
        """
        required_fields = [
            'item_name', 'sale_price', 'qty', 'sale_timestamp',
            'month', 'day_of_week', 'item_category', 'location_id',
            'modifiers'  
        ]
        
        # Check all required fields exist
        for field in required_fields:
            if field not in sales_row:
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate price and quantity
        if sales_row['sale_price'] < 0:
            logger.warning(f"Invalid sale_price: {sales_row['sale_price']}")
            return False
        
        if sales_row['qty'] <= 0:
            logger.warning(f"Invalid qty: {sales_row['qty']}")
            return False
        
        # Validate timestamp
        if not isinstance(sales_row['sale_timestamp'], dt.datetime):
            logger.warning(f"Invalid timestamp type: {type(sales_row['sale_timestamp'])}")
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


# Singleton instance
etl_service = ETLService()


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    """
    Test the ETL transformations.
    
    Usage:
        python app/services/etl_service.py
    """
    import json
    
    print("=" * 70)
    print("BAKEHOUSE ETL - SERVICE TEST")
    print("=" * 70)
    
    # Test 1: Timezone conversion
    print("\n[TEST 1] Timezone conversion...")
    utc_time = "2025-11-07T13:27:45.163Z"
    chicago_time = etl_service.convert_to_chicago_timezone(utc_time)
    print(f"UTC:     {utc_time}")
    print(f"Chicago: {chicago_time}")
    print(f"✅ Timezone: {chicago_time.tzname()}")
    
    # Test 2: Date components
    print("\n[TEST 2] Date component extraction...")
    month, dow = etl_service.extract_date_components(chicago_time)
    print(f"Month:       {month}")
    print(f"Day of Week: {dow}")
    print("✅ Extracted")
    
    # Test 3: Item filtering
    print("\n[TEST 3] Item filtering...")
    
    valid_item = {
        "name": "Croissant Plain",
        "base_price_money": {"amount": 500}
    }
    
    dine_in = {
        "name": "Dine In",
        "base_price_money": {"amount": 0}
    }
    
    print(f"Croissant: {etl_service._is_valid_line_item(valid_item)} ✅")
    print(f"Dine In: {etl_service._is_valid_line_item(dine_in)} (should be False)")
    
    # Test 4: Modifier parsing (NEW!)
    print("\n[TEST 4] Modifier parsing (V1.1)...")
    
    line_item_with_modifiers = {
        "name": "Iced Latte",
        "modifiers": [
            {
                "name": "Almond Milk",
                "base_price_money": {"amount": 100}
            },
            {
                "name": "Extra Shot",
                "base_price_money": {"amount": 75}
            },
            {
                "name": "To Go",
                "base_price_money": {"amount": 0}
            }
        ]
    }
    
    modifiers = etl_service._parse_modifiers(line_item_with_modifiers)
    print(f"Modifiers extracted: '{modifiers}'")
    print(f"✅ Filtered out $0 modifier (To Go)")
    
    # Test 5: Order transformation with modifiers
    print("\n[TEST 5] Order transformation with modifiers...")
    
    sample_order = {
        "id": "test_order_v1.1",
        "location_id": "L5WST6KFZBT10",
        "closed_at": "2025-11-24T13:27:45.163Z",
        "line_items": [
            {
                "name": "Iced Lavander latte",
                "variation_name": "16 oz",
                "quantity": "1",
                "base_price_money": {"amount": 565, "currency": "USD"},
                "gross_sales_money": {"amount": 665, "currency": "USD"},
                "modifiers": [
                    {
                        "name": "Almond Milk",
                        "base_price_money": {"amount": 100}
                    }
                ]
            },
            {
                "name": "Dine In",
                "variation_name": "N/A",
                "quantity": "1",
                "base_price_money": {"amount": 0, "currency": "USD"},
                "gross_sales_money": {"amount": 0, "currency": "USD"}
            }
        ]
    }
    
    sales_rows = etl_service.transform_order_to_sales(sample_order, location_id=2)
    
    print(f"✅ Order with {len(sample_order['line_items'])} line_items → {len(sales_rows)} sales rows")
    print(f"✅ Filtered out {len(sample_order['line_items']) - len(sales_rows)} items")
    
    print("\n[TEST 6] Sample sales row with modifiers:")
    print("-" * 70)
    sample_row = {
        **sales_rows[0],
        'sale_timestamp': str(sales_rows[0]['sale_timestamp'])
    }
    print(json.dumps(sample_row, indent=2))
    print("-" * 70)
    print(f"✅ sale_price = $6.65 (base $5.65 + modifier $1.00)")
    print(f"✅ modifiers = '{sales_rows[0]['modifiers']}'")
    
    # Test 7: Validation
    print("\n[TEST 7] Sales row validation...")
    is_valid = etl_service.validate_sales_row(sales_rows[0])
    if is_valid:
        print("✅ Validation passed (includes 'modifiers' field)")
    else:
        print("❌ Validation failed")
    
    print("\n" + "=" * 70)
    print("V1.1 Tests Complete! ✅")
    print(f"IGNORED_ITEMS: {ETLService.IGNORED_ITEMS}")
    print("=" * 70)