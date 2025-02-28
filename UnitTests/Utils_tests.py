from datetime import datetime

from utils.date_helpers import get_winter_summer_time_interval, validate_dates, format_datetime
import pytest

def test_get_winter_summer_time_interval():
    summertime = datetime(2025,6,21)
    wintertime = datetime(2025,12,21)

    summertime_interval = get_winter_summer_time_interval(summertime)
    wintertime_interval = get_winter_summer_time_interval(wintertime)

    assert summertime_interval == 2
    assert wintertime_interval == 1


def test_validate_dates_happy_flow():
    start_date = datetime(2025,1,1)
    end_date = datetime(2025,2,28)

    validation_response = validate_dates(start_datetime=start_date, end_datetime=end_date)

    assert validation_response == True

def test_validate_dates_wrong_order():
    start_date = datetime(2025,2,28)
    end_date = datetime(2025,1,1)

    with pytest.raises(ValueError, match='start_datetime and end_datetime must be in chronological order'):
        validate_dates(start_datetime=start_date, end_datetime=end_date)

def test_validate_dates_both_none():
    start_date = None
    end_date = None

    with pytest.raises(ValueError, match="One of both parameters 'start_datetime' or 'end_datetime' must be provided"):
        validate_dates(start_datetime=start_date, end_datetime=end_date)

def test_format_date_happy_flow():
    datetime_winter = datetime(2025,12,21)
    datetime_summer = datetime(2025,7,21)

    date_formatted_winter = format_datetime(datetime=datetime_winter)
    date_formatted_summer = format_datetime(datetime=datetime_summer)

    assert '2025-12-21T00:00:00.000+01:00' == date_formatted_winter
    assert '2025-07-21T00:00:00.000+02:00' == date_formatted_summer

