import unittest

from tap_postgres.tap import TapPostgres


class TestReplicationSlot(unittest.TestCase):
    def setUp(self):
        self.default_config = {
            "host": "localhost",
            "port": 5432,
            "dbname": "test_db",
            "user": "postgres",
            "password": "s3cr3t",
        }

    def test_default_slot_name(self):
        # Test backward compatibility when slot name is not provided.
        config = self.default_config
        tap = TapPostgres(config=config)
        self.assertEqual(
            tap.config.get("replication_slot_name", "tappostgres"), "tappostgres"
        )

    def test_custom_slot_name(self):
        # Test if the custom slot name is used.
        config = {**self.default_config, "replication_slot_name": "custom_slot"}
        tap = TapPostgres(config=config)
        self.assertEqual(tap.config["replication_slot_name"], "custom_slot")

    def test_multiple_slots(self):
        # Simulate using multiple configurations with different slot names.
        config_1 = {**self.default_config, "replication_slot_name": "slot_1"}
        config_2 = {**self.default_config, "replication_slot_name": "slot_2"}

        tap_1 = TapPostgres(config=config_1)
        tap_2 = TapPostgres(config=config_2)

        self.assertNotEqual(
            tap_1.config["replication_slot_name"],
            tap_2.config["replication_slot_name"],
        )
        self.assertEqual(tap_1.config["replication_slot_name"], "slot_1")
        self.assertEqual(tap_2.config["replication_slot_name"], "slot_2")

    def test_invalid_slot_name(self):
        # Test validation for invalid slot names (if any validation rules exist).
        invalid_config = {
            **self.default_config,
            "replication_slot_name": "invalid slot name!",
        }

        with self.assertRaises(ValueError):
            TapPostgres(config=invalid_config)
