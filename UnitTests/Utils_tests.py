import datetime

from utils.date_helpers import get_winter_summer_time_interval, parse_date, validate_dates, format_date
import pytest

def test_get_winter_summer_time_interval():
    summertime = '2025-06-21'
    wintertime = '2025-12-21'

    summertime_interval = get_winter_summer_time_interval(summertime)
    wintertime_interval = get_winter_summer_time_interval(wintertime)

    assert summertime_interval == 2
    assert wintertime_interval == 1

def test_parse_dates_happy_flow():
    date_str = '2025-01-01'
    date_parsed = parse_date(date_str=date_str)
    assert datetime.datetime(2025, 1, 1) == date_parsed

def test_parse_dates():
    date_str = '02-01-2025'
    with pytest.raises(ValueError):
        parse_date(date_str=date_str)

def test_validate_dates_happy_flow():
    start_date = '2025-01-01'
    end_date = '2025-02-28'

    start_date_validated, end_date_validated = validate_dates(start_date=start_date, end_date=end_date)

    assert start_date == start_date_validated
    assert end_date == end_date_validated

def test_validate_dates():
    start_date = '2025-02-28'
    end_date = '2025-01-01'

    with pytest.raises(ValueError, match='start_date and end_date must be in chronical order'):
        validate_dates(start_date=start_date, end_date=end_date)

def test_format_date_happy_flow():
    date_str_winter = '2025-12-21'
    date_str_summer = '2025-07-21'

    date_formatted_winter = format_date(date_str=date_str_winter)
    date_formatted_summer = format_date(date_str=date_str_summer)

    assert '2025-12-21T00:00:00.000+01:00' == date_formatted_winter
    assert '2025-07-21T00:00:00.000+02:00' == date_formatted_summer

