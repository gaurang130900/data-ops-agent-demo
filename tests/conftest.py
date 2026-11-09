"""
Shared pytest fixtures for the Autonomous Data Pipeline Healing Agent test suite.
"""

import pytest


@pytest.fixture
def airflow_log_response():
    """Sample Airflow task log containing a schema mismatch error."""
    return (
        "[2024-01-01 00:00:00] INFO - Running dbt model stg_customers\n"
        "[2024-01-01 00:00:01] ERROR - Database error: column 'customer_id' not found\n"
        "[2024-01-01 00:00:01] ERROR - Did you mean 'cust_id'?\n"
        "[2024-01-01 00:00:01] ERROR - Task failed with exit code 1\n"
    )


@pytest.fixture
def snowflake_schema_response():
    """
    Sample rows from SHOW COLUMNS IN TABLE.
    Each row: index 2 = column name, index 3 = data_type JSON string.
    """
    return [
        ("", "", "CUST_ID",        '{"type":"FIXED"}',   "", "", "", "", "", "", ""),
        ("", "", "CUSTOMER_NAME",  '{"type":"TEXT"}',    "", "", "", "", "", "", ""),
        ("", "", "EMAIL",          '{"type":"TEXT"}',    "", "", "", "", "", "", ""),
    ]


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set all required environment variables for the agent and tools."""
    monkeypatch.setenv("GEMINI_API_KEY",        "test-gemini-api-key")
    monkeypatch.setenv("GITHUB_TOKEN",          "test-github-token")
    monkeypatch.setenv("SNOWFLAKE_USER",        "test_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD",    "test_password")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT",     "test_account")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE",   "test_warehouse")
    monkeypatch.setenv("SNOWFLAKE_DATABASE",    "DATA_OPS_DEMO")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA",      "RAW")
