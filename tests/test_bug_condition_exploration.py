"""
Bug Condition Exploration Test for Snowflake Connector Migration

This test demonstrates the brittleness of the HTTP REST API approach in get_snowflake_schema.
It tests three failure scenarios that are common with the REST API implementation:
1. Enterprise security blocks (403 Forbidden)
2. Async query timeout (202 status with polling exhaustion)
3. Cryptic HTTP 500 errors

**CRITICAL**: This test is EXPECTED TO FAIL on unfixed code - failure confirms the bug exists.
When the code is fixed to use snowflake.connector, this test should PASS.

**Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5**
"""

import pytest
import sys
from unittest.mock import patch, MagicMock

from agent.tools import get_snowflake_schema


class TestBugConditionExploration:
    """
    Exploratory tests that demonstrate the brittleness of HTTP REST API approach.
    
    These tests EXPECT THE UNFIXED CODE TO FAIL - they prove the bug exists by:
    - Mocking realistic Snowflake REST API failure scenarios
    - Verifying that the fixed code uses snowflake.connector instead of HTTP requests
    
    When fixed, the code should use snowflake.connector and these scenarios won't occur.
    """

    def test_enterprise_security_block_403(self, monkeypatch, requests_mock):
        """
        Test Case 1: Enterprise security policies block REST API (403 Forbidden).
        
        Many enterprise Snowflake accounts disable REST API access for security.
        The unfixed code will fail with authentication error even with valid credentials.
        The fixed code using snowflake.connector should not make HTTP requests at all.
        
        **Expected on UNFIXED code**: Returns error dict due to 403 response
        **Expected on FIXED code**: Uses snowflake.connector (no HTTP requests made)
        """
        # Set up environment variables
        monkeypatch.setenv("SNOWFLAKE_USER", "valid_user")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "valid_password")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "enterprise_account")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "DATA_OPS_DEMO")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")

        # Mock the login endpoint to return 403 Forbidden (enterprise security block)
        login_url = "https://enterprise_account.snowflakecomputing.com/session/v1/login-request"
        requests_mock.post(
            login_url,
            status_code=403,
            json={
                "success": False,
                "message": "Forbidden: REST API access disabled by security policy"
            }
        )

        # VERIFICATION: Check if the code is using the standard connector (fixed)
        # Mock snowflake.connector properly
        with patch("snowflake.connector.connect") as mock_connect:
            # Create proper mock cursor and connection
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [
                ("", "", "CUST_ID", "TEXT", "", "", "", "", "", "", ""),
                ("", "", "NAME", "TEXT", "", "", "", "", "", "", ""),
            ]
            
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn

            result = get_snowflake_schema("RAW_CUSTOMERS")

            # FIXED CODE BEHAVIOR: Should use snowflake.connector
            # The mock_connect should be called, indicating connector usage
            if mock_connect.called:
                # This is the EXPECTED behavior after fix
                assert "columns" in result, "Fixed code should return columns using connector"
                assert len(result["columns"]) == 2
                assert result["columns"][0]["name"] == "CUST_ID"
                print("✓ Test PASSED: Code is using snowflake.connector (FIXED)")
            else:
                # This is the UNFIXED behavior - HTTP REST API was used
                # The 403 error should be returned
                assert "error" in result, "Unfixed code should return error due to 403"
                assert "403" in result["error"] or "Forbidden" in result["error"] or "login failed" in result["error"].lower()
                print("✗ Test FAILED: Code is using HTTP REST API and hit 403 error (UNFIXED)")
                pytest.fail("Bug confirmed: HTTP REST API approach fails with enterprise security (403). This test will pass when fixed with snowflake.connector.")

    def test_async_query_timeout(self, monkeypatch, requests_mock):
        """
        Test Case 2: Async query timeout (202 status with polling exhaustion).
        
        The unfixed code manually polls for async query completion up to 30 iterations.
        If the query never completes, it times out with an error.
        The fixed code using snowflake.connector handles this transparently.
        
        **Expected on UNFIXED code**: Returns timeout error after 30 polling attempts
        **Expected on FIXED code**: Uses snowflake.connector (no HTTP requests made)
        """
        # Set up environment variables
        monkeypatch.setenv("SNOWFLAKE_USER", "valid_user")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "valid_password")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test_account")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "DATA_OPS_DEMO")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")

        # Mock successful authentication
        login_url = "https://test_account.snowflakecomputing.com/session/v1/login-request"
        requests_mock.post(
            login_url,
            status_code=200,
            json={
                "success": True,
                "data": {"token": "test-token"}
            }
        )

        # Mock statements endpoint to return 202 (async execution)
        sql_url = "https://test_account.snowflakecomputing.com/api/v2/statements"
        requests_mock.post(
            sql_url,
            status_code=202,
            json={
                "statementHandle": "test-handle-12345",
                "statementStatusUrl": "test-handle-12345"
            }
        )

        # Mock polling endpoint to never complete (simulate timeout)
        poll_url = "https://test_account.snowflakecomputing.com/api/v2/statements/test-handle-12345"
        requests_mock.get(
            poll_url,
            status_code=202,
            json={
                "statementHandle": "test-handle-12345",
                "message": "Query still running"
            }
        )

        # VERIFICATION: Check if the code is using the standard connector (fixed)
        with patch("snowflake.connector.connect") as mock_connect:
            # Create proper mock cursor and connection
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [
                ("", "", "CUST_ID", "TEXT", "", "", "", "", "", "", ""),
            ]
            
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn

            result = get_snowflake_schema("LARGE_TABLE")

            # FIXED CODE BEHAVIOR: Should use snowflake.connector
            if mock_connect.called:
                # This is the EXPECTED behavior after fix
                assert "columns" in result, "Fixed code should return columns using connector"
                print("✓ Test PASSED: Code is using snowflake.connector (FIXED)")
            else:
                # This is the UNFIXED behavior - HTTP REST API was used
                # The timeout error should be returned
                assert "error" in result, "Unfixed code should return error due to timeout"
                assert "timed out" in result["error"].lower() or "timeout" in result["error"].lower()
                print("✗ Test FAILED: Code is using HTTP REST API and hit timeout error (UNFIXED)")
                pytest.fail("Bug confirmed: HTTP REST API approach times out on async queries. This test will pass when fixed with snowflake.connector.")

    def test_cryptic_http_500_error(self, monkeypatch, requests_mock):
        """
        Test Case 3: Cryptic HTTP 500 errors with minimal information.
        
        The unfixed code receives generic HTTP 500 errors that are hard to debug.
        The fixed code using snowflake.connector provides structured exceptions.
        
        **Expected on UNFIXED code**: Returns error with "HTTP 500" message
        **Expected on FIXED code**: Uses snowflake.connector (no HTTP requests made)
        """
        # Set up environment variables
        monkeypatch.setenv("SNOWFLAKE_USER", "valid_user")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "valid_password")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test_account")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "DATA_OPS_DEMO")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")

        # Mock successful authentication
        login_url = "https://test_account.snowflakecomputing.com/session/v1/login-request"
        requests_mock.post(
            login_url,
            status_code=200,
            json={
                "success": True,
                "data": {"token": "test-token"}
            }
        )

        # Mock statements endpoint to return 500 with minimal error text
        sql_url = "https://test_account.snowflakecomputing.com/api/v2/statements"
        requests_mock.post(
            sql_url,
            status_code=500,
            text="Internal Server Error"
        )

        # VERIFICATION: Check if the code is using the standard connector (fixed)
        with patch("snowflake.connector.connect") as mock_connect:
            # Create proper mock cursor and connection
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [
                ("", "", "CUST_ID", "TEXT", "", "", "", "", "", "", ""),
            ]
            
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn

            result = get_snowflake_schema("CUSTOMERS")

            # FIXED CODE BEHAVIOR: Should use snowflake.connector
            if mock_connect.called:
                # This is the EXPECTED behavior after fix
                assert "columns" in result, "Fixed code should return columns using connector"
                print("✓ Test PASSED: Code is using snowflake.connector (FIXED)")
            else:
                # This is the UNFIXED behavior - HTTP REST API was used
                # The 500 error should be returned
                assert "error" in result, "Unfixed code should return error due to 500"
                assert "500" in result["error"] or "Internal Server Error" in result["error"]
                print("✗ Test FAILED: Code is using HTTP REST API and hit 500 error (UNFIXED)")
                pytest.fail("Bug confirmed: HTTP REST API approach provides cryptic HTTP 500 errors. This test will pass when fixed with snowflake.connector.")

    def test_missing_env_vars_error_handling_preserved(self, monkeypatch):
        """
        Test Case 4: Missing environment variables should return descriptive error.
        
        This tests preservation of existing error handling behavior.
        Both unfixed and fixed code should return the same error format.
        
        **Expected on UNFIXED code**: Returns error dict listing missing variables
        **Expected on FIXED code**: Returns error dict listing missing variables (SAME)
        """
        # Clear all Snowflake env vars
        for var in ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
                    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"]:
            monkeypatch.delenv(var, raising=False)

        result = get_snowflake_schema("RAW_CUSTOMERS")

        # Both unfixed and fixed code should handle this the same way
        assert isinstance(result, dict), "Should return dict"
        assert "error" in result, "Should return error dict"
        assert "Missing" in result["error"] or "missing" in result["error"].lower()
        assert "SNOWFLAKE_USER" in result["error"] or "environment" in result["error"].lower()
        print("✓ Error handling for missing env vars is correct")
