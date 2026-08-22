from unittest.mock import MagicMock, patch

from tap_postgres.tap import TapPostgres


@patch("tap_postgres.client.boto3.Session")
def test_aws_iam_auth_token_injection(mock_session_class):
    # Mock the boto3 Session and RDS client
    mock_session = MagicMock()
    mock_client = MagicMock()
    mock_session.client.return_value = mock_client
    mock_session_class.return_value = mock_session

    mock_client.generate_db_auth_token.return_value = "mocked-aws-token"

    # Configuration with AWS IAM enabled and password omitted
    config = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "database": "test_db",
        "aws_iam_auth": True,
        "aws_region": "us-east-1",
    }

    # 1. Verify Tap initialization succeeds without a password config
    tap = TapPostgres(config=config, setup_mapper=False)
    assert tap.config.get("aws_iam_auth") is True

    # 2. Retrieve the connector
    connector = tap.connector

    # 3. Simulate a database connection parameters payload
    cparams = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "",  # Empty initial password
    }

    # 4. Directly test the provide_token method (completely bypassing database connections)
    connector.provide_token(None, None, None, cparams)

    # 5. Assertions:
    # Verify that the connection password was replaced with our mocked AWS token
    assert cparams["password"] == "mocked-aws-token"

    # Verify that the boto3 rds client generated the token with correct configs
    mock_client.generate_db_auth_token.assert_called_once_with(
        DBHostname="localhost",
        Port=5432,
        DBUsername="test_user",
        Region="us-east-1",
    )
