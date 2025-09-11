import pytest
from hypothesis import given
from hypothesis import strategies as st
from singer_sdk.exceptions import ConfigValidationError

from tap_postgres.tap import REPLICATION_SLOT_PATTERN, TapPostgres
from tests.settings import DB_SQLALCHEMY_URL


@pytest.fixture
def default_config():
    return {"sqlalchemy_url": DB_SQLALCHEMY_URL}


def test_default_slot_name(default_config: dict):
    """Test backward compatibility when slot name is not provided."""
    tap = TapPostgres(config=default_config, setup_mapper=False)
    assert tap.config.get("replication_slot_name", "tappostgres") == "tappostgres"


@given(st.from_regex(REPLICATION_SLOT_PATTERN))
def test_custom_slot_name(s: str):
    """Test if the custom slot name is used."""
    config = {
        "sqlalchemy_url": DB_SQLALCHEMY_URL,
        "replication_slot_name": s,
    }
    tap = TapPostgres(config=config, setup_mapper=False)
    assert tap.config["replication_slot_name"] == s


def test_multiple_slots(default_config: dict):
    """Simulate using multiple configurations with different slot names."""
    config_1 = {**default_config, "replication_slot_name": "slot_1"}
    config_2 = {**default_config, "replication_slot_name": "slot_2"}

    tap_1 = TapPostgres(config=config_1, setup_mapper=False)
    tap_2 = TapPostgres(config=config_2, setup_mapper=False)

    assert tap_1.config["replication_slot_name"] != tap_2.config["replication_slot_name"]
    assert tap_1.config["replication_slot_name"] == "slot_1"
    assert tap_2.config["replication_slot_name"] == "slot_2"


def test_invalid_slot_name(default_config: dict):
    """Test validation for invalid slot names."""
    invalid_slot_name = "invalid slot name!"
    invalid_config = {
        **default_config,
        "replication_slot_name": invalid_slot_name,
    }

    with pytest.raises(ConfigValidationError, match="does not match") as exc_info:
        TapPostgres(config=invalid_config, setup_mapper=False)

    errors = exc_info.value.errors
    assert len(errors) == 1
    assert invalid_slot_name in errors[0]
