import datetime
import logging
import psycopg2.extensions

logger = logging.getLogger(__name__)

class PostgresTypeAdapter:
    # OIDs for PostgreSQL types. Query pg_type if these are not standard for your DB version.
    DATE_OID = 1082  # date
    TIMESTAMP_OID = 1114  # timestamp without timezone
    TIMESTAMPTZ_OID = 1184  # timestamp with timezone

    def __init__(self):
        # The logger can be module-level or instance-level.
        # Using the module-level logger here.
        self.logger = logger 

    def safe_parse_date(self, value, cur):
        """Safely parse PostgreSQL date strings, returning None for errors or special values."""
        if value is None:
            return None

        if isinstance(value, bytes):
            try:
                str_value = value.decode('utf-8')
            except UnicodeDecodeError:
                self.logger.warning(f"Could not decode bytes to UTF-8 for date parsing: {value!r}")
                return None
        else:
            str_value = str(value)

        if str_value == 'infinity' or str_value == '-infinity':
            self.logger.debug(f"Parsed PostgreSQL special date value '{str_value}' as None.")
            return None

        try:
            return datetime.datetime.strptime(str_value, '%Y-%m-%d').date()
        except (ValueError, OverflowError) as e:
            self.logger.warning(f"Could not parse date string '{str_value}' to a valid Python date. Error: {e}. Returning None.")
            return None

    def safe_parse_datetime(self, value, cur):
        """Safely parse PostgreSQL timestamp/timestamptz strings, returning None for errors or special values."""
        if value is None:
            return None

        if isinstance(value, bytes):
            try:
                str_value = value.decode('utf-8')
            except UnicodeDecodeError:
                self.logger.warning(f"Could not decode bytes to UTF-8 for datetime parsing: {value!r}")
                return None
        else:
            str_value = str(value)

        if str_value == 'infinity' or str_value == '-infinity':
            self.logger.debug(f"Parsed PostgreSQL special datetime value '{str_value}' as None.")
            return None

        try:
            # datetime.fromisoformat handles standard ISO 8601 formats from PostgreSQL
            return datetime.datetime.fromisoformat(str_value)
        except (ValueError, OverflowError) as e:
            self.logger.warning(f"Could not parse datetime string '{str_value}' to a valid Python datetime. Error: {e}. Returning None.")
            return None

    def register_parsers(self):
        """Registers custom parsers for date, timestamp, and timestamptz types with psycopg2."""
        # psycopg2.extensions.new_typecaster seems to be missing in the environment,
        # trying with psycopg2.extensions.new_type instead.
        
        # For DATE
        date_type = psycopg2.extensions.new_type((self.DATE_OID,), "CUSTOM_DATE", self.safe_parse_date)
        psycopg2.extensions.register_type(date_type)
        self.logger.info(f"Custom safe date parser registered for OID {self.DATE_OID} using new_type.")

        # For TIMESTAMP and TIMESTAMPTZ
        # We need to register them separately if using new_type with a single OID list for the type name
        
        # TIMESTAMP
        timestamp_type = psycopg2.extensions.new_type((self.TIMESTAMP_OID,), "CUSTOM_TIMESTAMP", self.safe_parse_datetime)
        psycopg2.extensions.register_type(timestamp_type)
        self.logger.info(f"Custom safe datetime parser registered for OID {self.TIMESTAMP_OID} (timestamp) using new_type.")

        # TIMESTAMPTZ
        timestamptz_type = psycopg2.extensions.new_type((self.TIMESTAMPTZ_OID,), "CUSTOM_TIMESTAMPTZ", self.safe_parse_datetime)
        psycopg2.extensions.register_type(timestamptz_type)
        self.logger.info(f"Custom safe datetime parser registered for OID {self.TIMESTAMPTZ_OID} (timestamptz) using new_type.")
