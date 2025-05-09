"""A Singer tap for Postgres, built with the Meltano SDK."""

# Register custom date parsers to handle bad date values
from .postgres_type_adapter import PostgresTypeAdapter
adapter = PostgresTypeAdapter()
adapter.register_parsers() 