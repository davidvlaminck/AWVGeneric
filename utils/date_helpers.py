from datetime import timedelta, datetime

import pytz


def get_winter_summer_time_interval(date: datetime, timezone='Europe/Brussels') -> int:
    """
    Determines if the given datetime is in summer time (DST) or winter time (Standard Time).

    :param date: datetime
    :param timezone: The time zone to check (default: "Europe/Brussels")
    :return: 2 during "Summer Time (DST)" and 1 during "Winter Time (Standard Time)"
    """
    # Assign timezone information
    tz = pytz.timezone(timezone)
    dt_tz = tz.localize(date)

    # Check daylight saving time (DST)
    if dt_tz.dst() != timedelta(0):
        return 2  # "Summer Time (DST)"
    else:
        return 1  # "Winter Time (Standard Time)"


def validate_dates(start_datetime: datetime = None, end_datetime: datetime = None) -> bool:
    """
    Validates that at least one date is provided, converts string dates to datetime,
    and ensures that start_date (if given) is earlier than end_date.

    :param start_datetime: start date
    :param end_datetime: end date
    :return: True on successful validation
    :raises ValueError: If validation fails
    """
    # One of both parameters must be provided
    if start_datetime is None and end_datetime is None:
        raise ValueError("One of both parameters 'start_datetime' or 'end_datetime' must be provided")
    # Check if both are present and if the start_date is earlier than end_date
    elif start_datetime and end_datetime and start_datetime >= end_datetime:
        raise ValueError("start_datetime and end_datetime must be in chronological order")
    return True


def format_datetime(datetime: datetime) -> str:
    """ Formats datetime to a string, including the correct time_interval during winter/summer time

    :param datetime: datetime
    :return: date as string '%Y-%m-%dT00:00:00.000+00:00'
    """
    hour_interval = get_winter_summer_time_interval(date=datetime)
    return f'{datetime.strftime("%Y-%m-%dT%H:%M:%S")}.000+0{hour_interval}:00'

