"""
Monthly ETL Script - Orchestrates the complete ETL pipeline
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

# Reduce verbosity for service loggers (only show warnings and errors)
logging.getLogger('app.services.etl_service').setLevel(logging.WARNING)
logging.getLogger('app.services.square_service').setLevel(logging.WARNING)
logging.getLogger('app.services.database_service').setLevel(logging.INFO)


class MonthlyETL:
    """
    Orchestrates the monthly ETL process.
    """
    
    def __init__(self):
        """Initialize the ETL orchestrator with all required services."""
        self.square_service = square_service
        self.etl_service = etl_service
        self.db_service = DatabaseService()
        
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
        """Calculate the start and end dates for the previous month."""
        today = datetime.now()
        first_of_current_month = today.replace(day=1) #today is the first day of current month
        last_day_of_previous_month = first_of_current_month - timedelta(days=1) #time delta of 1 day
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)# replace is a literal day of the month
        
        start_date = first_day_of_previous_month.strftime('%Y-%m-%d')
        end_date = last_day_of_previous_month.strftime('%Y-%m-%d')
        
        logger.info(f"Calculated previous month: {start_date} to {end_date}")
        return start_date, end_date
    
    
    def get_date_range_from_env(self) -> Tuple[str, str]:
        """Get date range from environment variables or calculate previous month."""
        start_date = os.getenv('START_DATE')
        end_date = os.getenv('END_DATE')
        
        if start_date and end_date:
            logger.info(f"Using custom date range: {start_date} to {end_date}")
            return start_date, end_date
        elif start_date or end_date:
            logger.warning("Both START_DATE and END_DATE must be provided. Using previous month.")
            return self.calculate_previous_month()
        else:
            return self.calculate_previous_month()
    
    
    def get_locations_to_process(self) -> List[Dict]:
        """Get the list of locations to process based on LOCATION_FILTER env var."""
        all_locations = self.db_service.get_all_locations()
   
        return all_locations
    
    
    
    def is_test_mode(self) -> bool:
        """Check if running in test mode."""
        test_mode = os.getenv('TEST', 'false').lower()
        is_test = test_mode in ['true', '1', 'yes']
        
        if is_test:
            logger.info("⚠️  TEST MODE ENABLED - 5 orders per location")
        
        return is_test
    
    
    def process_location(
        self,
        location: Dict,
        start_date: str,
        end_date: str,
        test_mode: bool
    ) -> Dict:
        """Process ETL for a single location."""
        location_name = location['name']
        location_id = location['id']
        square_id = location['square_id']
        
        print(f"Processing: {location_name}")
     
        
        result = {
            'location_name': location_name,
            'orders_fetched': 0,
            'sales_rows_created': 0,
            'rows_inserted': 0,
            'success': False,
            'error': None
        }
        
        try:
            # Fetch orders
            print(f"[1/3] Fetching orders from Square API...")
            orders = self.square_service.fetch_orders_by_date_range(
                location_id=square_id,
                start_date=start_date,
                end_date=end_date,
                test=test_mode
            )
            result['orders_fetched'] = len(orders)
            print(f"    ✓ Fetched {len(orders)} orders")
            
            if not orders:
                print(f"      ℹ No orders found")
                result['success'] = True
                return result
            
            # Transform orders
            print(f"[2/3] Transforming to sales rows...")
            sales_data = self.etl_service.transform_orders_batch(
                orders=orders,
                location_id=location_id
            )
            result['sales_rows_created'] = len(sales_data)
            print(f"      ✓ Created {len(sales_data)} sales rows")
            
            if not sales_data:
                print(f"      ℹ No sales data created")
                result['success'] = True
                return result
            
            # Insert into database
            print(f"[3/3] Inserting into database...")
            rows_inserted = self.db_service.bulk_insert_sales(sales_data)
            result['rows_inserted'] = rows_inserted
            print(f"      ✓ Inserted {rows_inserted} rows")
            
            result['success'] = True
            print(f"\n✅ {location_name} completed successfully")
            
            return result
            
        except SquareAPIError as e:
            error_msg = f"Square API error: {str(e)}"
            print(f"\n❌ {location_name} failed: {error_msg}")
            logger.error(error_msg)
            result['error'] = error_msg
            return result
            
        except ETLValidationError as e:
            error_msg = f"ETL validation error: {str(e)}"
            print(f"\n❌ {location_name} failed: {error_msg}")
            logger.error(error_msg)
            result['error'] = error_msg
            return result
            
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            print(f"\n❌ {location_name} failed: {error_msg}")
            logger.error(error_msg, exc_info=True)
            result['error'] = error_msg
            return result
    
    
    def run(self) -> bool:
        """Execute the complete monthly ETL process."""
        self.stats['start_time'] = datetime.now()
        
        print("\n" + "#"*60)
        print("# MONTHLY ETL PROCESS")
        print("#"*60)
        
        # Test database connection
        print("\n[0/3] Testing database connection...")
        if not self.db_service.test_connection():
            print("❌ Database connection failed. Aborting.")
            return False
        print("      ✓ Database connected")
        
        # Get configuration
        start_date, end_date = self.get_date_range_from_env()
        locations = self.get_locations_to_process()
        test_mode = self.is_test_mode()
        
        if not locations:
            print("❌ No locations to process. Aborting.")
            return False
        
        self.stats['total_locations'] = len(locations)
        
        # Print configuration
        print("\n" + "="*60)
        print("CONFIGURATION")
        print("="*60)
        print(f"Date range: {start_date} to {end_date}")
        print(f"Locations: {len(locations)}")
        for loc in locations:
            print(f"  - {loc['name']} (ID: {loc['id']})")
        print(f"Test mode: {'YES (5 orders)' if test_mode else 'NO (full)'}")
        print("="*60)
        
        # Process each location
        results = []
        
        for location in locations:
            result = self.process_location(
                location=location,
                start_date=start_date,
                end_date=end_date,
                test_mode=test_mode
            )
            results.append(result)
            
            self.stats['total_orders'] += result['orders_fetched']
            self.stats['total_sales_rows'] += result['rows_inserted']
            
            if not result['success']:
                self.stats['failed_locations'] += 1
        
        # Print summary
        self.stats['end_time'] = datetime.now()
        self.print_summary(results, start_date, end_date, test_mode)
        
        return self.stats['failed_locations'] == 0
    
    
    def print_summary(self, results: List[Dict], start_date: str, end_date: str, test_mode: bool):
        """Print summary report."""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        print("\n" + "="*60)
        print("ETL SUMMARY REPORT")
        print("="*60)
        
        print("\nPER-LOCATION RESULTS:")
        print("-" * 60)
        for result in results:
            status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
            print(f"{status} - {result['location_name']}")
            print(f"  Orders fetched: {result['orders_fetched']}")
            print(f"  Sales rows: {result['sales_rows_created']}")
            print(f"  Rows inserted: {result['rows_inserted']}")
            if result['error']:
                print(f"  Error: {result['error']}")
            print()
        
        print("="*60)
        print("OVERALL STATISTICS")
        print("="*60)
        print(f"Date range: {start_date} to {end_date}")
        print(f"Test mode: {'YES' if test_mode else 'NO'}")
        print(f"Locations processed: {self.stats['total_locations']}")
        print(f"Locations failed: {self.stats['failed_locations']}")
        print(f"Total orders: {self.stats['total_orders']}")
        print(f"Total rows inserted: {self.stats['total_sales_rows']}")
        print(f"Duration: {duration:.1f}s")
        print("="*60)
        
        if self.stats['failed_locations'] == 0:
            print("\n✅ ETL COMPLETED SUCCESSFULLY")
        else:
            print(f"\n⚠️  COMPLETED WITH {self.stats['failed_locations']} FAILURES")
        
        print("="*60 + "\n")


def main():
    """Main entry point."""
    try:
        etl = MonthlyETL()
        success = etl.run()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()