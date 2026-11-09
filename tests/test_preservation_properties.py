"""
Preservation Property Tests for Snowflake Connector Migration

This test suite follows observation-first methodology to capture and verify behaviors
that MUST BE PRESERVED during the bugfix. These tests should PASS on the UNFIXED code
to establish the baseline, and continue to PASS on the FIXED code to prevent regressions.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**
"""

import pytest
import logging
import os
from unittest.mock import patch, MagicMock, call
from hypothesis import given, settings, strategies as st, assume, HealthCheck

from agent.tools import get_snowflake_schema


# ---------------------------------------------------------------------------
# Test Case 1: Return Structure for Valid Table Names
# ---------------------------------------------------------------------------

@given(
    table_format=st.sampled_from([
        "TABLE_NAME",                           # Simple format
        "SCHEMA.TABLE_NAME",                    # Partially qualified
        "DATABASE.SCHEMA.TABLE_NAME"            # Fully qualified
    ]),
    column_name=st.from_regex(r'[A-Z][A-Z0-9_]{0,15}', fullmatch=True),
    data_type=st.sampled_from(["TEXT", "NUMBER", "DATE", "TIMESTAMP", "BOOLEAN", "VARIANT"])
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_return_structure_for_valid_tables(monkeypatch, table_format, column_name, data_type):
    """
    **Property 2: Preservation - Return Format**
    **Validates: Requirements 3.1**
    
    For all valid table names in various formats, verify return structure is {"columns": [...]}.
    This ensures the fix preserves the return format expected by callers.
    
    **EXPECTED OUTCOME**: PASS on unfixed code (establishes baseline to preserve)
    """
    # Set up environment
    monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test_password")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test_account")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "DATA_OPS_DEMO")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")

    # Mock snowflake.connector
    with patch("snowflake.connector.connect") as mock_connect:
        # Create mock connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("table", "schema", column_name, data_type, "", "", "", "", "", "", "")
        ]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        result = get_snowflake_schema(table_format)

    # PRESERVATION CHECK: Return structure must be {"columns": [...]}
    assert isinstance(result, dict), "Return value must be a dict"
    assert "columns" in result, "Return dict must contain 'columns' key"
    assert isinstance(result["columns"], list), "columns value must be a list"
    
    # Each column should have "name" and "type" keys
    if len(result["columns"]) > 0:
        assert "name" in result["columns"][0], "Each column must have 'name' key"
        assert "type" in result["columns"][0], "Each column must have 'type' key"


# ---------------------------------------------------------------------------
# Test Case 2: Error Format for Missing Environment Variables
# ---------------------------------------------------------------------------

@given(
    missing_var=st.sampled_from([
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA"
    ])
)
@settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_error_format_missing_env_vars(monkeypatch, missing_var):
    """
    **Property 2: Preservation - Error Format**
    **Validates: Requirements 3.2, 3.4**
    
    For all missing environment variable combinations, verify error format is {"error": "..."}
    and that the error message lists the missing variables descriptively.
    
    **EXPECTED OUTCOME**: PASS on unfixed code (establishes baseline to preserve)
    """
    # Set up all env vars except the missing one
    env_vars = {
        "SNOWFLAKE_USER": "test_user",
        "SNOWFLAKE_PASSWORD": "test_password",
        "SNOWFLAKE_ACCOUNT": "test_account",
        "SNOWFLAKE_WAREHOUSE": "test_warehouse",
        "SNOWFLAKE_DATABASE": "DATA_OPS_DEMO",
        "SNOWFLAKE_SCHEMA": "RAW"
    }
    
    for var, value in env_vars.items():
        if var != missing_var:
            monkeypatch.setenv(var, value)
        else:
            monkeypatch.delenv(var, raising=False)

    result = get_snowflake_schema("TEST_TABLE")

    # PRESERVATION CHECK: Error format must be {"error": "..."}
    assert isinstance(result, dict), "Return value must be a dict"
    assert "error" in result, "Error response must contain 'error' key"
    assert isinstance(result["error"], str), "error value must be a string"
    
    # Error message should be descriptive and mention missing variables
    error_msg = result["error"]
    assert "Missing" in error_msg or "missing" in error_msg.lower(), \
        "Error message should indicate missing variables"
    assert missing_var in error_msg, \
        f"Error message should mention the missing variable: {missing_var}"


# ---------------------------------------------------------------------------
# Test Case 3: Table Name Parsing for Different Formats
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table_name,expected_db,expected_schema,expected_table", [
    ("DATABASE.SCHEMA.TABLE", "DATABASE", "SCHEMA", "TABLE"),
    ("SCHEMA.TABLE", "DATA_OPS_DEMO", "SCHEMA", "TABLE"),  # Uses env DATABASE
    ("TABLE", "DATA_OPS_DEMO", "RAW", "TABLE"),            # Uses env DATABASE and SCHEMA
])
def test_property_table_name_parsing(monkeypatch, table_name, expected_db, expected_schema, expected_table):
    """
    **Property 2: Preservation - Table Name Parsing**
    **Validates: Requirements 3.3**
    
    For all table name formats (fully qualified, partially qualified, simple),
    verify parsing produces correct database, schema, table components.
    
    **EXPECTED OUTCOME**: PASS on unfixed code (establishes baseline to preserve)
    """
    # Set up environment
    monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test_password")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test_account")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "DATA_OPS_DEMO")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")

    # Capture the SQL statement to verify parsing
    captured_sql = []
    
    # Mock snowflake.connector
    with patch("snowflake.connector.connect") as mock_connect:
        # Create mock cursor that captures SQL
        mock_cursor = MagicMock()
        
        def capture_execute(sql):
            captured_sql.append(sql)
        
        mock_cursor.execute.side_effect = capture_execute
        mock_cursor.fetchall.return_value = [
            ("table", "schema", "TEST_COL", "TEXT", "", "", "", "", "", "", "")
        ]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        result = get_snowflake_schema(table_name)

    # PRESERVATION CHECK: Query should reference correct database, schema, table
    assert len(captured_sql) > 0, "SQL query should have been executed"
    sql = captured_sql[0]
    
    # Verify the SQL query references the correct components
    assert expected_db.upper() in sql.upper(), f"SQL should reference database: {expected_db}"
    assert expected_schema.upper() in sql.upper(), f"SQL should reference schema: {expected_schema}"
    assert expected_table.upper() in sql.upper(), f"SQL should reference table: {expected_table}"
    
    # Also verify successful return structure
    assert "columns" in result, "Should return columns on success"


# ---------------------------------------------------------------------------
# Test Case 4: Logging Behavior Preservation
# ---------------------------------------------------------------------------

def test_property_logging_calls_on_success(monkeypatch, caplog):
    """
    **Property 2: Preservation - Logging Behavior**
    **Validates: Requirements 3.6**
    
    Verify logging calls occur at appropriate points: logger.info for success.
    
    **EXPECTED OUTCOME**: PASS on unfixed code (establishes baseline to preserve)
    """
    # Set up environment
    monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test_password")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test_account")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "DATA_OPS_DEMO")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")

    # Mock snowflake.connector
    with patch("snowflake.connector.connect") as mock_connect:
        # Create mock connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("table", "schema", "CUST_ID", "TEXT", "", "", "", "", "", "", ""),
            ("table", "schema", "NAME", "TEXT", "", "", "", "", "", "", "")
        ]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Capture logs
        with caplog.at_level(logging.INFO):
            result = get_snowflake_schema("RAW_CUSTOMERS")

    # PRESERVATION CHECK: Should have INFO level logs for success
    assert "columns" in result, "Should successfully return columns"
    
    # Verify logging occurred
    log_messages = [record.message for record in caplog.records]
    assert any("Fetching Snowflake schema" in msg for msg in log_messages), \
        "Should log initial fetch message"
    assert any("Retrieved" in msg or "column" in msg.lower() for msg in log_messages), \
        "Should log successful retrieval"


def test_property_logging_calls_on_error(monkeypatch, caplog):
    """
    **Property 2: Preservation - Logging Behavior on Error**
    **Validates: Requirements 3.6**
    
    Verify logging calls occur at appropriate points: logger.error for failures.
    
    **EXPECTED OUTCOME**: PASS on unfixed code (establishes baseline to preserve)
    """
    # Set up environment - missing variables to trigger error
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_WAREHOUSE", raising=False)
    monkeypatch.delenv("SNOWFLAKE_DATABASE", raising=False)
    monkeypatch.delenv("SNOWFLAKE_SCHEMA", raising=False)

    # Capture logs
    with caplog.at_level(logging.ERROR):
        result = get_snowflake_schema("TEST_TABLE")

    # PRESERVATION CHECK: Should have ERROR level logs for failure
    assert "error" in result, "Should return error dict"
    
    # Verify error logging occurred
    log_messages = [record.message for record in caplog.records if record.levelname == "ERROR"]
    assert len(log_messages) > 0, "Should log error messages"
    assert any("Missing" in msg or "missing" in msg for msg in log_messages), \
        "Should log missing environment variables error"


# ---------------------------------------------------------------------------
# Test Case 5: Function Signature Preservation
# ---------------------------------------------------------------------------

def test_property_function_signature():
    """
    **Property 2: Preservation - Function Signature**
    **Validates: Requirements 3.5**
    
    Verify function signature remains: get_snowflake_schema(table_name: str) -> dict
    
    **EXPECTED OUTCOME**: PASS on unfixed code (establishes baseline to preserve)
    """
    import inspect
    from agent.tools import get_snowflake_schema
    
    # PRESERVATION CHECK: Function signature
    sig = inspect.signature(get_snowflake_schema)
    
    # Should have exactly one parameter: table_name
    params = list(sig.parameters.keys())
    assert len(params) == 1, "Function should have exactly one parameter"
    assert params[0] == "table_name", "Parameter should be named 'table_name'"
    
    # Parameter should be annotated as str
    param_annotation = sig.parameters["table_name"].annotation
    assert param_annotation == str, "Parameter should be annotated as str"
    
    # Return type should be annotated as dict
    assert sig.return_annotation == dict, "Return type should be annotated as dict"


# ---------------------------------------------------------------------------
# Test Case 6: Complete Return Format with Multiple Columns
# ---------------------------------------------------------------------------

def test_property_complete_return_format(monkeypatch):
    """
    **Property 2: Preservation - Complete Return Format**
    **Validates: Requirements 3.1**
    
    Verify complete return format with multiple columns matches expected structure.
    
    **EXPECTED OUTCOME**: PASS on unfixed code (establishes baseline to preserve)
    """
    # Set up environment
    monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test_password")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test_account")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "DATA_OPS_DEMO")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")

    # Mock snowflake.connector
    with patch("snowflake.connector.connect") as mock_connect:
        # Create mock connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("table", "schema", "CUST_ID", "NUMBER", "", "", "", "", "", "", ""),
            ("table", "schema", "FIRST_NAME", "TEXT", "", "", "", "", "", "", ""),
            ("table", "schema", "LAST_NAME", "TEXT", "", "", "", "", "", "", ""),
            ("table", "schema", "EMAIL", "TEXT", "", "", "", "", "", "", ""),
            ("table", "schema", "CREATED_AT", "TIMESTAMP", "", "", "", "", "", "", "")
        ]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        result = get_snowflake_schema("RAW_CUSTOMERS")

    # PRESERVATION CHECK: Verify exact format
    assert isinstance(result, dict), "Return value must be a dict"
    assert set(result.keys()) == {"columns"}, "Dict should only contain 'columns' key"
    assert isinstance(result["columns"], list), "columns value must be a list"
    assert len(result["columns"]) == 5, "Should return all 5 columns"
    
    # Verify each column has correct structure
    for col in result["columns"]:
        assert isinstance(col, dict), "Each column must be a dict"
        assert set(col.keys()) == {"name", "type"}, "Each column should have 'name' and 'type' keys"
        assert isinstance(col["name"], str), "Column name must be a string"
        assert isinstance(col["type"], str), "Column type must be a string"
    
    # Verify specific column values
    assert result["columns"][0]["name"] == "CUST_ID"
    assert result["columns"][0]["type"] == "NUMBER"


# ---------------------------------------------------------------------------
# Test Case 7: Error Format for Connection Failures
# ---------------------------------------------------------------------------

def test_property_error_format_connection_failure(monkeypatch):
    """
    **Property 2: Preservation - Error Format for Connection Failures**
    **Validates: Requirements 3.2**
    
    Verify error format {"error": "..."} is preserved for connection failures.
    
    **EXPECTED OUTCOME**: PASS on unfixed code (establishes baseline to preserve)
    """
    # Set up environment
    monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test_password")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test_account")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "DATA_OPS_DEMO")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")

    # Mock connection error
    import snowflake.connector.errors
    with patch("snowflake.connector.connect") as mock_connect:
        mock_connect.side_effect = snowflake.connector.errors.DatabaseError("Network unreachable")
        
        result = get_snowflake_schema("RAW_CUSTOMERS")

    # PRESERVATION CHECK: Error format must be {"error": "..."}
    assert isinstance(result, dict), "Return value must be a dict"
    assert "error" in result, "Error response must contain 'error' key"
    assert isinstance(result["error"], str), "error value must be a string"
    assert "Error:" in result["error"], "Error message should start with 'Error:'"


# ---------------------------------------------------------------------------
# Test Case 8: Never Raise Exceptions
# ---------------------------------------------------------------------------

@given(
    table_name=st.text(min_size=1, max_size=100)
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None)
def test_property_never_raises_exceptions(monkeypatch, table_name):
    """
    **Property 2: Preservation - Exception Handling**
    **Validates: Requirements 3.5**
    
    Verify the function never raises exceptions, always returns dict.
    
    **EXPECTED OUTCOME**: PASS on unfixed code (establishes baseline to preserve)
    """
    # Set up minimal environment (may be invalid)
    monkeypatch.setenv("SNOWFLAKE_USER", "test")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "test")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "test")

    # Mock snowflake.connector to prevent actual connections
    import snowflake.connector.errors
    with patch("snowflake.connector.connect") as mock_connect:
        mock_connect.side_effect = snowflake.connector.errors.DatabaseError("Connection failed")
        
        # PRESERVATION CHECK: Should never raise, always return dict
        try:
            result = get_snowflake_schema(table_name)
            assert isinstance(result, dict), "Must always return a dict"
            assert "columns" in result or "error" in result, "Must return either columns or error"
        except Exception as e:
            pytest.fail(f"Function raised exception: {e}. Should return error dict instead.")
