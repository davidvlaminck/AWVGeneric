from datetime import timedelta, datetime

import pytz


def get_winter_summer_time_interval(date_str, timezone='Europe/Brussels') -> int:
    """
    Determines if the given date is in summer time (DST) or winter time (Standard Time).

    :param date_str: The date as a string (e.g., "2024-06-15")
    :param timezone: The time zone to check (default: "Europe/Brussels")
    :return: 2 during "Summer Time (DST)" and 1 during "Winter Time (Standard Time)"
    """
    # Convert string date to datetime object
    dt = datetime.strptime(date_str, '%Y-%m-%d')

    # Assign timezone information
    tz = pytz.timezone(timezone)
    dt_tz = tz.localize(dt)

    # Check daylight saving time (DST)
    if dt_tz.dst() != timedelta(0):
        return 2  # "Summer Time (DST)"
    else:
        return 1  # "Winter Time (Standard Time)"


def validate_dates(start_date: str = None, end_date: str = None):
    """
    Validates that at least one date is provided, converts string dates to datetime,
    and ensures that start_date (if given) is earlier than end_date.

    :param start_date: The start date as a string (optional)
    :param end_date: The end date as a string (optional)
    :return: A tuple (start_date, end_date) as datetime objects
    :raises ValueError: If validation fails
    """
    # Ensure at least one date is provided
    if start_date is None and end_date is None:
        raise ValueError("At least one of start_date or end_date must be provided.")

    # Convert string dates to datetime objects (or None if empty)
    start_date_parsed = parse_date(date_str=start_date)
    end_date_parsed = parse_date(date_str=end_date)

    # Check if start_date is earlier than end_date (if both are provided)
    if start_date_parsed and end_date_parsed and start_date_parsed >= end_date_parsed:
        raise ValueError("start_date and end_date must be in chronical order")

    return start_date, end_date


def parse_date(date_str: str) -> datetime:
    """Parse date from String to Datetime format

    :param date_str: date in string format, structure '%Y-%m-%d'
    :return: date in datetime format
    """
    try:
        return datetime.strptime(date_str, '%Y-%m-%d') if date_str else None
    except ValueError as e:
        raise ValueError(
            'Invalid date format. Expected format: %Y-%m-%d'
        ) from e


def format_date(date_str: str) -> str:
    """ Formats date to a complete date, including the correct time_interval during winter/summer time

    :param date_str: '%Y-%m-%d'
    :return: date as string '%Y-%m-%d'
    """
    hour_interval = get_winter_summer_time_interval(date_str=date_str)
    return f'{date_str}T00:00:00.000+0{hour_interval}:00'
