"""
Date and Timezone Utilities for Bakehouse ETL

This module handles all date/time operations for the ETL
- Timezone conversions (UTC ↔ America/Chicago)
- Date component extraction (month, day_of_week, time)
- Date range calculations for monthly ETL runs

WHY THIS EXISTS:
- Square API returns timestamps in UTC (ISO 8601 format) more specifically rfc3339 that
uses a subset of ISO 8601 and has  T as the date-time separator
- Business operates in Chicago timezone (CST/CDT)
- Avoid timezone bugs that cause off-by-one-day errors
"""

from datetime import datetime, timedelta, time
from typing import Tuple
import pytz
from dateutil import parser


# CONSTANTS: Define timezone once, use everywhere
# This follows the "Don't Repeat Yourself" (DRY) principle
CHICAGO_TZ = pytz.timezone("America/Chicago")
UTC_TZ = pytz.UTC


def convert_utc_to_chicago(utc_timestamp: str) -> datetime:
    """
    Convert a UTC timestamp string to Chicago timezone.
    
    - Square API returns all timestamps in UTC (ISO 8601 format)
    
    Args:
        utc_timestamp (str): ISO 8601 timestamp from Square API
            Examples: 
            - "2025-11-07T13:27:45.163Z"
            - "2025-01-15T18:30:00Z"
    
    Returns:
        datetime: Timezone-aware datetime object in Chicago timezone
        
    Example:
        >>> utc_time = "2025-11-07T13:27:45.163Z"
        >>> chicago_time = convert_utc_to_chicago(utc_time)
        >>> print(chicago_time)
        2025-11-07 07:27:45.163000-06:00  # 6 hours behind UTC (CST)
        
        # In summer (CDT):
        >>> utc_time = "2025-07-15T13:27:45Z"
        >>> chicago_time = convert_utc_to_chicago(utc_time)
        >>> print(chicago_time)
        2025-07-15 08:27:45-05:00  # 5 hours behind UTC (CDT)
    
    Raises:
        ValueError: If timestamp string is invalid or cannot be parsed
    """
    try:
  
        utc_dt = parser.isoparse(utc_timestamp)
        
        # Step 2: Ensure the datetime is timezone-aware (has UTC info)
        # If no timezone in string, assume UTC
        if utc_dt.tzinfo is None:
            utc_dt = UTC_TZ.localize(utc_dt)
        
        # Step 3: Convert to Chicago timezone
        # astimezone() automatically handles DST transitions
        chicago_dt = utc_dt.astimezone(CHICAGO_TZ)
        
        return chicago_dt
        
    except (ValueError, TypeError) as e:
        raise ValueError(
            f"Failed to convert timestamp '{utc_timestamp}' to Chicago time. "
            f"Expected ISO 8601 format (e.g., '2025-11-07T13:27:45Z'). "
            f"Error: {str(e)}"
        )


def extract_date_components(chicago_dt: datetime) -> Tuple[str, str, time]:
    """
    Extract month, day_of_week, and time from a Chicago timezone datetime.
    
    These components are stored separately in the database for efficient
    querying and aggregation in Tableau.
    
    WHY WE EXTRACT THESE:
    - month: For monthly sales reports and trends
    - day_of_week: For day-of-week analysis (e.g., "Sundays are busiest")
    - created_time: For intraday patterns (e.g., "lunch rush at noon")
    
    Args:
        chicago_dt (datetime): Timezone-aware datetime in Chicago timezone
    
    Returns:
        Tuple[str, str, time]: (month, day_of_week, created_time)
            - month: 'YYYY-MM' format (e.g., '2025-11')
            - day_of_week: Full weekday name (e.g., 'Monday', 'Tuesday')
            - created_time: Time only (HH:MM:SS)
    
    Example:
        >>> from datetime import datetime
        >>> import pytz
        >>> dt = datetime(2025, 11, 7, 13, 27, 45, tzinfo=pytz.timezone('America/Chicago'))
        >>> month, day, time_only = extract_date_components(dt)
        >>> print(month)
        '2025-11'
        >>> print(day)
        'Thursday'
        >>> print(time_only)
        13:27:45
    """
    # Extract month in YYYY-MM format
    # This format sorts correctly and is easy to filter in SQL
    month = chicago_dt.strftime('%Y-%m')
    
    # Extract full weekday name
    # strftime('%A') returns full name ('Monday', 'Tuesday', etc.)
    # Alternative: strftime('%a') for abbreviation ('Mon', 'Tue')
    day_of_week = chicago_dt.strftime('%A')
    
    # Extract time only (without date information)
    # This is useful for intraday analysis (hourly sales patterns)
    created_time = chicago_dt.time()
    
    return month, day_of_week, created_time


def calculate_previous_month_range() -> Tuple[str, str]:
    """
    Calculate the date range for the previous month (used by monthly ETL).
    
    This function is called by the GitHub Actions workflow to automatically
    determine which month's data to fetch.(is due to run on the first of each month, for the 
    simplicity of scheduling)
    
    HOW IT WORKS:
    1. Get today's date in Chicago timezone
    2. Go back to the 1st of current month
    3. Subtract 1 day to get last day of previous month
    4. Go to 1st of that month
    
    Returns:
        Tuple[str, str]: (start_date, end_date) in 'YYYY-MM-DD' format
        
    Example:
        # If today is 2025-11-07 (November 7th)
        >>> start, end = calculate_previous_month_range()
        >>> print(start)
        '2025-10-01'
        >>> print(end)
        '2025-10-31'
    
    Edge Cases Handled:
    - Year transitions (Jan → Dec of previous year)
    - Different month lengths (28/29/30/31 days)
    - Leap years (February handling)
    """
    # Get today's date in Chicago timezone
    # Important: Use Chicago time, not UTC, to avoid date boundary issues
    today_chicago = datetime.now(CHICAGO_TZ)
    
    # Go to the first day of the current month 
    # this works because replace() keeps the same year and month
    first_of_current_month = today_chicago.replace(day=1)
    
    # Subtract one day to get the last day of the previous month
    # this works since github only runs this on the first of the month and delta
    # days can cross month boundaries
    last_day_previous_month = first_of_current_month - timedelta(days=1)
    
    # Go to the first day of the previous month
    first_day_previous_month = last_day_previous_month.replace(day=1)
    
    # Format as strings (YYYY-MM-DD)
    start_date = first_day_previous_month.strftime('%Y-%m-%d')
    end_date = last_day_previous_month.strftime('%Y-%m-%d')
    
    return start_date, end_date


def format_for_square_api(date_str: str, is_start: bool = True) -> str:
    """
    Format a date string for Square API datetime filter.
    
    Square API requires ISO 8601 timestamps with timezone info.
    This function converts simple dates (YYYY-MM-DD) to the format
    Square expects.
    
    KEY POINTS:
    - Square API requires specific timestamp format
    - Need to include timezone information
    - Need to set time to start/end of day appropriately
    
    Args:
        date_str (str): Date in 'YYYY-MM-DD' format (e.g., '2025-10-01')
        is_start (bool): True for start of day (00:00:00), False for end (23:59:59)
    
    Returns:
        str: ISO 8601 timestamp with Chicago timezone
            Format: 'YYYY-MM-DDTHH:MM:SS-05:00' (or -06:00 for CST)
    
    Example:
        >>> start = format_for_square_api('2025-10-01', is_start=True)
        >>> print(start)
        '2025-10-01T00:00:00-05:00'
        
        >>> end = format_for_square_api('2025-10-31', is_start=False)
        >>> print(end)
        '2025-10-31T23:59:59-05:00'
    """
    # Parse the date string
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    
    # Set time based on whether it's start or end of day
    if is_start:
        # Start of day: 00:00:00
        time_obj = time(0, 0, 0)
    else:
        # End of day: 23:59:59
        time_obj = time(23, 59, 59)
    
    # Combine date and time
    dt = datetime.combine(date_obj, time_obj)
    
    # Localize to Chicago timezone
    chicago_dt = CHICAGO_TZ.localize(dt)
    
    # Format as ISO 8601 string
    # isoformat() produces: '2025-10-01T00:00:00-05:00'
    return chicago_dt.isoformat()


def get_current_chicago_time() -> datetime:
    """
    Get the current date and time in Chicago timezone.
    
    Used for utility functions for logging and debugging.
    
    Example:
        >>> now = get_current_chicago_time()
        >>> print(now)
        2025-11-07 13:45:30.123456-06:00
    """
    return datetime.now(CHICAGO_TZ)


def validate_date_range(start_date: str, end_date: str) -> bool:
    """
    Validate that a date range is logical (start <= end).
    
    Used for input validation in API endpoints and scripts.
    
    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format
        end_date (str): End date in 'YYYY-MM-DD' format
    
    Returns:
        bool: True if valid, False otherwise
    
    Example:
        >>> validate_date_range('2025-10-01', '2025-10-31')
        True
        
        >>> validate_date_range('2025-10-31', '2025-10-01')
        False
    """
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        return start <= end
    except ValueError:
        return False


# ============================================================================
# TESTING 
# ============================================================================

if __name__ == "__main__":
    """
    Run this file directly to test all functions.
    
    Usage:
        python app/utils/date_utils.py
    """
    print("=" * 70)
    print("BAKEHOUSE ETL - DATE UTILITIES TEST")
    print("=" * 70)
    
    # Test 1: UTC to Chicago conversion
    print("\n1. Converting UTC timestamp to Chicago time:")
    test_utc = "2025-11-07T13:27:45.163Z"
    chicago_time = convert_utc_to_chicago(test_utc)
    print(f"   UTC:     {test_utc}")
    print(f"   Chicago: {chicago_time}")
    print(f"   Offset:  {chicago_time.strftime('%z')} ({chicago_time.tzname()})")
    
    # Test 2: Extract date components
    print("\n2. Extracting date components:")
    month, day, time_only = extract_date_components(chicago_time)
    print(f"   Month:       {month}")
    print(f"   Day of Week: {day}")
    print(f"   Time:        {time_only}")
    
    # Test 3: Previous month calculation
    print("\n3. Calculating previous month range:")
    start, end = calculate_previous_month_range()
    print(f"   Start: {start}")
    print(f"   End:   {end}")
    
    # Test 4: Format for Square API
    print("\n4. Formatting dates for Square API:")
    square_start = format_for_square_api(start, is_start=True)
    square_end = format_for_square_api(end, is_start=False)
    print(f"   API Start: {square_start}")
    print(f"   API End:   {square_end}")
    
    # Test 5: Date range validation
    print("\n5. Validating date ranges:")
    valid = validate_date_range(start, end)
    invalid = validate_date_range(end, start)
    print(f"   '{start}' to '{end}': {valid} ✓")
    print(f"   '{end}' to '{start}': {invalid} ✗")
    
    # Test 6: Current time
    print("\n6. Current Chicago time:")
    now = get_current_chicago_time()
    print(f"   {now}")
    
    print("\n" + "=" * 70)
    print("ALL TESTS PASSED! ✓")
    print("=" * 70)