import pytest
from singer_sdk.exceptions import ConfigValidationError

from tap_postgres.tap import TapPostgres
from tests.settings import DB_SQLALCHEMY_URL


@pytest.fixture
def default_config():
    return {"sqlalchemy_url": DB_SQLALCHEMY_URL}


def test_default_slot_name(default_config):
    # Test backward compatibility when slot name is not provided.
    tap = TapPostgres(config=default_config)
    assert tap.config.get("replication_slot_name", "tappostgres") == "tappostgres"


def test_custom_slot_name(default_config):
    # Test if the custom slot name is used.
    config = {**default_config, "replication_slot_name": "custom_slot"}
    tap = TapPostgres(config=config)
    assert tap.config["replication_slot_name"] == "custom_slot"


def test_multiple_slots(default_config):
    # Simulate using multiple configurations with different slot names.
    config_1 = {**default_config, "replication_slot_name": "slot_1"}
    config_2 = {**default_config, "replication_slot_name": "slot_2"}

    tap_1 = TapPostgres(config=config_1)
    tap_2 = TapPostgres(config=config_2)

    assert (
        tap_1.config["replication_slot_name"] != tap_2.config["replication_slot_name"]
    )
    assert tap_1.config["replication_slot_name"] == "slot_1"
    assert tap_2.config["replication_slot_name"] == "slot_2"


def test_invalid_slot_name(default_config):
    # Test validation for invalid slot names (if any validation rules exist).
    invalid_config = {
        **default_config,
        "replication_slot_name": "invalid slot name!",
    }

    with pytest.raises(ConfigValidationError) as exc_info:
        TapPostgres(config=invalid_config)

    # Verify that the error message contains information about the cause
    assert "'invalid slot name!' does not match '^(?!pg_)[A-Za-z0-9_]{1,63}$'" in str(
        exc_info.value
    )
