"""
Unit tests for agent/tools.py.

Tests cover all three tool functions:
  - get_airflow_logs
  - get_snowflake_schema
  - raise_github_pr
"""

import pytest
import requests
import requests_mock as requests_mock_module
from unittest.mock import patch, MagicMock, PropertyMock
from github import GithubException

from agent.tools import get_airflow_logs, get_snowflake_schema, raise_github_pr


# ---------------------------------------------------------------------------
# get_airflow_logs
# ---------------------------------------------------------------------------

class TestGetAirflowLogs:

    def test_get_airflow_logs_success(self, requests_mock):
        """Successful log retrieval returns the content string from JSON response."""
        url = (
            "http://localhost:8080/api/v1/dags/test_dag"
            "/dagRuns/latest/taskInstances/test_task/logs/1"
        )
        requests_mock.get(url, json={"content": "log output"})

        result = get_airflow_logs("test_dag", "test_task")

        assert result == "log output"

    def test_get_airflow_logs_http_error(self, requests_mock):
        """A non-200 response results in an error string."""
        url = (
            "http://localhost:8080/api/v1/dags/test_dag"
            "/dagRuns/latest/taskInstances/test_task/logs/1"
        )
        requests_mock.get(url, status_code=404, text="Not Found")

        result = get_airflow_logs("test_dag", "test_task")

        assert "Error:" in result

    def test_get_airflow_logs_connection_error(self, requests_mock):
        """A ConnectionError results in an error string."""
        url = (
            "http://localhost:8080/api/v1/dags/test_dag"
            "/dagRuns/latest/taskInstances/test_task/logs/1"
        )
        requests_mock.get(url, exc=requests.exceptions.ConnectionError)

        result = get_airflow_logs("test_dag", "test_task")

        assert "Error:" in result

    def test_get_airflow_logs_returns_string(self, requests_mock):
        """Return type is always str regardless of outcome."""
        url = (
            "http://localhost:8080/api/v1/dags/any_dag"
            "/dagRuns/latest/taskInstances/any_task/logs/1"
        )
        requests_mock.get(url, exc=requests.exceptions.ConnectionError)

        result = get_airflow_logs("any_dag", "any_task")

        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# get_snowflake_schema
# ---------------------------------------------------------------------------

class TestGetSnowflakeSchema:

    def test_get_snowflake_schema_success(self, monkeypatch):
        """Successful schema retrieval parses column name and type correctly."""
        monkeypatch.setenv("SNOWFLAKE_USER", "u")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "p")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "myaccount")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "w")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "d")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "s")

        # Mock snowflake.connector.connect and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ["DB", "SCHEMA", "CUST_ID", "TEXT", None, None, None, None],
            ["DB", "SCHEMA", "NAME", "VARCHAR", None, None, None, None],
        ]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch("snowflake.connector.connect", return_value=mock_conn):
            result = get_snowflake_schema("DB.SCHEMA.TABLE")

        assert "columns" in result
        assert len(result["columns"]) == 2
        assert result["columns"][0]["name"] == "CUST_ID"
        assert result["columns"][0]["type"] == "TEXT"
        assert result["columns"][1]["name"] == "NAME"
        assert result["columns"][1]["type"] == "VARCHAR"
        
        # Verify cursor and connection were closed
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_get_snowflake_schema_missing_env(self, monkeypatch):
        """Missing env vars return a dict with 'error' key."""
        for var in ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
                    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"]:
            monkeypatch.delenv(var, raising=False)

        result = get_snowflake_schema("DB.SCHEMA.TABLE")

        assert "error" in result

    def test_get_snowflake_schema_connection_failure(self, monkeypatch):
        """A connection error returns a dict with 'error' key."""
        monkeypatch.setenv("SNOWFLAKE_USER", "u")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "p")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "myaccount")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "w")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "d")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "s")

        with patch("snowflake.connector.connect", side_effect=Exception("Connection failed")):
            result = get_snowflake_schema("DB.SCHEMA.TABLE")

        assert "error" in result

    def test_get_snowflake_schema_returns_dict(self, monkeypatch):
        """Return type is always dict."""
        monkeypatch.delenv("SNOWFLAKE_USER", raising=False)

        result = get_snowflake_schema("DB.SCHEMA.TABLE")

        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# raise_github_pr
# ---------------------------------------------------------------------------

class TestRaiseGithubPr:

    def test_raise_github_pr_missing_token(self, monkeypatch):
        """Missing GITHUB_TOKEN returns an error string."""
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)

        result = raise_github_pr("owner/repo", "path/file.sql", "SELECT 1")

        assert "Error:" in result

    def test_raise_github_pr_auth_failure(self, monkeypatch):
        """GithubException during authentication returns an error string."""
        monkeypatch.setenv("GITHUB_TOKEN", "bad-token")

        mock_g = MagicMock()
        mock_user = MagicMock()
        mock_user.login = PropertyMock(
            side_effect=GithubException(401, {"message": "Bad credentials"}, None)
        )
        mock_g.get_user.return_value = mock_user
        # Accessing .login raises the exception
        type(mock_g.get_user.return_value).login = PropertyMock(
            side_effect=GithubException(401, {"message": "Bad credentials"}, None)
        )

        with patch("agent.tools.Github", return_value=mock_g):
            result = raise_github_pr("owner/repo", "path/file.sql", "SELECT 1")

        assert "Error:" in result

    def test_raise_github_pr_success(self, monkeypatch):
        """A fully mocked PyGithub chain returns the PR html_url."""
        monkeypatch.setenv("GITHUB_TOKEN", "valid-token")

        mock_pr = MagicMock()
        mock_pr.html_url = "https://github.com/owner/repo/pull/42"

        mock_branch = MagicMock()
        mock_branch.commit.sha = "abc123"

        mock_file_contents = MagicMock()
        mock_file_contents.sha = "def456"

        mock_repo = MagicMock()
        mock_repo.get_branch.return_value = mock_branch
        mock_repo.create_git_ref.return_value = MagicMock()
        mock_repo.get_contents.return_value = mock_file_contents
        mock_repo.update_file.return_value = MagicMock()
        mock_repo.create_pull.return_value = mock_pr

        mock_g = MagicMock()
        mock_g.get_user.return_value.login = "test-user"
        mock_g.get_repo.return_value = mock_repo

        with patch("agent.tools.Github", return_value=mock_g):
            result = raise_github_pr(
                "owner/repo",
                "dbt_project/models/stg_customers.sql",
                "SELECT cust_id AS customer_id FROM RAW_CUSTOMERS",
            )

        assert result == "https://github.com/owner/repo/pull/42"

    def test_raise_github_pr_branch_name_contains_timestamp(self, monkeypatch):
        """Created branch name matches the 'fix/schema-mismatch-<timestamp>' pattern."""
        monkeypatch.setenv("GITHUB_TOKEN", "valid-token")

        captured_branch = {}

        mock_pr = MagicMock()
        mock_pr.html_url = "https://github.com/owner/repo/pull/1"

        mock_branch = MagicMock()
        mock_branch.commit.sha = "abc123"

        mock_file_contents = MagicMock()
        mock_file_contents.sha = "def456"

        mock_repo = MagicMock()
        mock_repo.get_branch.return_value = mock_branch

        def capture_ref(ref, sha):
            captured_branch["ref"] = ref
            return MagicMock()

        mock_repo.create_git_ref.side_effect = capture_ref
        mock_repo.get_contents.return_value = mock_file_contents
        mock_repo.update_file.return_value = MagicMock()
        mock_repo.create_pull.return_value = mock_pr

        mock_g = MagicMock()
        mock_g.get_user.return_value.login = "test-user"
        mock_g.get_repo.return_value = mock_repo

        with patch("agent.tools.Github", return_value=mock_g):
            raise_github_pr(
                "owner/repo",
                "dbt_project/models/stg_customers.sql",
                "SELECT 1",
            )

        assert "refs/heads/fix/schema-mismatch-" in captured_branch.get("ref", "")

    def test_raise_github_pr_pr_title(self, monkeypatch):
        """The PR is created with the correct title."""
        monkeypatch.setenv("GITHUB_TOKEN", "valid-token")

        captured_title = {}

        mock_pr = MagicMock()
        mock_pr.html_url = "https://github.com/owner/repo/pull/1"

        mock_branch = MagicMock()
        mock_branch.commit.sha = "abc123"

        mock_file_contents = MagicMock()
        mock_file_contents.sha = "def456"

        mock_repo = MagicMock()
        mock_repo.get_branch.return_value = mock_branch
        mock_repo.create_git_ref.return_value = MagicMock()
        mock_repo.get_contents.return_value = mock_file_contents
        mock_repo.update_file.return_value = MagicMock()

        def capture_pull(**kwargs):
            captured_title["title"] = kwargs.get("title")
            return mock_pr

        mock_repo.create_pull.side_effect = capture_pull

        mock_g = MagicMock()
        mock_g.get_user.return_value.login = "test-user"
        mock_g.get_repo.return_value = mock_repo

        with patch("agent.tools.Github", return_value=mock_g):
            raise_github_pr(
                "owner/repo",
                "dbt_project/models/stg_customers.sql",
                "SELECT 1",
            )

        assert captured_title.get("title") == "Fix: Schema mismatch in stg_customers.sql"
