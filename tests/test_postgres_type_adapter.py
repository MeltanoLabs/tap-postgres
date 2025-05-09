import datetime
import pytest
from tap_postgres.postgres_type_adapter import PostgresTypeAdapter

# The 'cur' argument is not used by our parsing functions, so None is fine.
MOCK_CURSOR = None

@pytest.fixture(scope="module")
def adapter():
    """Provides a PostgresTypeAdapter instance for the tests."""
    # Note: This will trigger the registration in PostgresTypeAdapter if it's set to auto-register on init.
    # For these parsing unit tests, the registration state itself isn't directly tested,
    # only the parsing methods' behavior.
    return PostgresTypeAdapter()

# Test cases for safe_parse_date
DATE_TEST_CASES = [
    ("2023-10-26", datetime.date(2023, 10, 26)),
    ("infinity", None),
    ("-infinity", None),
    ("202229-01-01", None),  # Year out of Python's date range
    ("0000-01-01", None),    # Year out of Python's date range (min year is 1)
    ("2023-044-01", None),   # Malformed month
    ("not-a-date", None),
    (None, None),
    (b"2023-11-15", datetime.date(2023, 11, 15)),
    (b"\\xff\\xfe", None),  # Invalid UTF-8 bytes
    (b"2023-30-30", None),  # Malformed date in bytes
    ("0001-01-01", datetime.date(1, 1, 1)),      # Min valid date
    ("9999-12-31", datetime.date(9999, 12, 31)), # Max valid date
]

@pytest.mark.parametrize("value, expected", DATE_TEST_CASES)
def test_safe_parse_date(adapter, value, expected):
    """Tests the safe_parse_date method of PostgresTypeAdapter."""
    result = adapter.safe_parse_date(value, MOCK_CURSOR)
    assert result == expected

# Test cases for safe_parse_datetime
DATETIME_TEST_CASES = [
    ("2023-10-26T10:30:00", datetime.datetime(2023, 10, 26, 10, 30, 0)),
    ("2023-10-26 10:30:00", datetime.datetime(2023, 10, 26, 10, 30, 0)), # Space separator
    ("2023-10-26T10:30:00Z", datetime.datetime(2023, 10, 26, 10, 30, 0, tzinfo=datetime.timezone.utc)),
    ("2023-10-26T10:30:00+01:00", datetime.datetime(2023, 10, 26, 10, 30, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=3600)))),
    ("2023-10-26T10:30:00.123456", datetime.datetime(2023, 10, 26, 10, 30, 0, 123456)),
    ("2023-10-26 10:30:00.123456-05:00", datetime.datetime(2023, 10, 26, 10, 30, 0, 123456, tzinfo=datetime.timezone(datetime.timedelta(seconds=-18000)))),
    ("infinity", None),
    ("-infinity", None),
    ("10000-01-01T00:00:00", None),  # Year out of Python's datetime range
    ("0000-01-01T00:00:00", None),    # Year out of Python's datetime range (min year is 1)
    ("not-a-datetime", None),
    (None, None),
    (b"2023-11-15T12:00:00", datetime.datetime(2023, 11, 15, 12, 0, 0)),
    (b"2023-11-15T12:00:00-05:00", datetime.datetime(2023, 11, 15, 12, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=-18000)))),
    (b"\\xff\\xfeT00:00:00", None),  # Invalid UTF-8 bytes
    ("0001-01-01T00:00:00", datetime.datetime(1, 1, 1, 0, 0, 0)),  # Min valid datetime
    ("9999-12-31T23:59:59.999999", datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)), # Max valid datetime
]

@pytest.mark.parametrize("value, expected", DATETIME_TEST_CASES)
def test_safe_parse_datetime(adapter, value, expected):
    """Tests the safe_parse_datetime method of PostgresTypeAdapter."""
    result = adapter.safe_parse_datetime(value, MOCK_CURSOR)
    assert result == expected 