"""
Unit tests for agent/tools.py.

Tests cover all five tool functions:
  - get_airflow_logs
  - get_snowflake_schema
  - read_code_file
  - execute_test_query
  - raise_github_pr
"""

import pytest
import requests
import requests_mock as requests_mock_module
from unittest.mock import patch, MagicMock, PropertyMock
from github import GithubException

from agent.tools import (
    get_airflow_logs,
    get_snowflake_schema,
    read_code_file,
    execute_test_query,
    raise_github_pr,
)


# ---------------------------------------------------------------------------
# get_airflow_logs
# ---------------------------------------------------------------------------

class TestGetAirflowLogs:

    # The Airflow v2 flow is two-step: list dagRuns, then fetch logs for a run.
    _RUNS_URL = (
        "http://localhost:8080/api/v2/dags/test_dag/dagRuns"
        "?limit=10&order_by=-start_date"
    )

    def _logs_url(self, run_id: str = "run1", task_id: str = "test_task") -> str:
        return (
            f"http://localhost:8080/api/v2/dags/test_dag"
            f"/dagRuns/{run_id}/taskInstances/{task_id}/logs/1"
        )

    def test_get_airflow_logs_success(self, requests_mock):
        """Successful log retrieval returns the joined event text from the v2 response."""
        requests_mock.get(
            "http://localhost:8080/api/v2/dags/test_dag/dagRuns",
            json={"dag_runs": [{"dag_run_id": "run1", "state": "failed"}]},
        )
        requests_mock.get(
            self._logs_url("run1"),
            json={"content": [{"event": "log output"}]},
        )

        result = get_airflow_logs("test_dag", "test_task")

        assert result == "log output"

    def test_get_airflow_logs_http_error(self, requests_mock):
        """A non-200 response on the dagRuns listing results in an error string."""
        requests_mock.get(
            "http://localhost:8080/api/v2/dags/test_dag/dagRuns",
            status_code=404,
            text="Not Found",
        )

        result = get_airflow_logs("test_dag", "test_task")

        assert "Error:" in result

    def test_get_airflow_logs_connection_error(self, requests_mock):
        """A ConnectionError results in an error string."""
        requests_mock.get(
            "http://localhost:8080/api/v2/dags/test_dag/dagRuns",
            exc=requests.exceptions.ConnectionError,
        )

        result = get_airflow_logs("test_dag", "test_task")

        assert "Error:" in result

    def test_get_airflow_logs_returns_string(self, requests_mock):
        """Return type is always str regardless of outcome."""
        requests_mock.get(
            "http://localhost:8080/api/v2/dags/test_dag/dagRuns",
            exc=requests.exceptions.ConnectionError,
        )

        result = get_airflow_logs("test_dag", "test_task")

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

    def test_raise_github_pr_repo_not_found(self, monkeypatch):
        """Repository not found returns an error string."""
        monkeypatch.setenv("GITHUB_TOKEN", "valid-token")

        mock_g = MagicMock()
        mock_g.get_user.return_value.login = "test-user"
        mock_g.get_repo.side_effect = GithubException(404, {"message": "Not Found"}, None)

        with patch("agent.tools.Github", return_value=mock_g):
            result = raise_github_pr("owner/nonexistent", "file.sql", "SELECT 1")

        assert "Error:" in result
        assert "nonexistent" in result or "not found" in result.lower() or "not accessible" in result.lower()

    def test_raise_github_pr_branch_creation_failure(self, monkeypatch):
        """Branch creation failure returns an error string."""
        monkeypatch.setenv("GITHUB_TOKEN", "valid-token")

        mock_branch = MagicMock()
        mock_branch.commit.sha = "abc123"

        mock_repo = MagicMock()
        mock_repo.get_branch.return_value = mock_branch
        mock_repo.create_git_ref.side_effect = GithubException(
            422, {"message": "Reference already exists"}, None
        )

        mock_g = MagicMock()
        mock_g.get_user.return_value.login = "test-user"
        mock_g.get_repo.return_value = mock_repo

        with patch("agent.tools.Github", return_value=mock_g):
            result = raise_github_pr("owner/repo", "file.sql", "SELECT 1")

        assert "Error:" in result

    def test_raise_github_pr_file_not_found(self, monkeypatch):
        """File not found in repo returns an error string."""
        monkeypatch.setenv("GITHUB_TOKEN", "valid-token")

        mock_branch = MagicMock()
        mock_branch.commit.sha = "abc123"

        mock_repo = MagicMock()
        mock_repo.get_branch.return_value = mock_branch
        mock_repo.create_git_ref.return_value = MagicMock()
        mock_repo.get_contents.side_effect = GithubException(404, {"message": "Not Found"}, None)

        mock_g = MagicMock()
        mock_g.get_user.return_value.login = "test-user"
        mock_g.get_repo.return_value = mock_repo

        with patch("agent.tools.Github", return_value=mock_g):
            result = raise_github_pr("owner/repo", "nonexistent/file.sql", "SELECT 1")

        assert "Error:" in result

    def test_raise_github_pr_update_file_failure(self, monkeypatch):
        """File update failure returns an error string."""
        monkeypatch.setenv("GITHUB_TOKEN", "valid-token")

        mock_branch = MagicMock()
        mock_branch.commit.sha = "abc123"

        mock_file_contents = MagicMock()
        mock_file_contents.sha = "def456"

        mock_repo = MagicMock()
        mock_repo.get_branch.return_value = mock_branch
        mock_repo.create_git_ref.return_value = MagicMock()
        mock_repo.get_contents.return_value = mock_file_contents
        mock_repo.update_file.side_effect = GithubException(
            409, {"message": "Conflict"}, None
        )

        mock_g = MagicMock()
        mock_g.get_user.return_value.login = "test-user"
        mock_g.get_repo.return_value = mock_repo

        with patch("agent.tools.Github", return_value=mock_g):
            result = raise_github_pr("owner/repo", "file.sql", "SELECT 1")

        assert "Error:" in result

    def test_raise_github_pr_pr_creation_failure(self, monkeypatch):
        """PR creation failure returns an error string."""
        monkeypatch.setenv("GITHUB_TOKEN", "valid-token")

        mock_branch = MagicMock()
        mock_branch.commit.sha = "abc123"

        mock_file_contents = MagicMock()
        mock_file_contents.sha = "def456"

        mock_repo = MagicMock()
        mock_repo.get_branch.return_value = mock_branch
        mock_repo.create_git_ref.return_value = MagicMock()
        mock_repo.get_contents.return_value = mock_file_contents
        mock_repo.update_file.return_value = MagicMock()
        mock_repo.create_pull.side_effect = GithubException(
            422, {"message": "Validation Failed"}, None
        )

        mock_g = MagicMock()
        mock_g.get_user.return_value.login = "test-user"
        mock_g.get_repo.return_value = mock_repo

        with patch("agent.tools.Github", return_value=mock_g):
            result = raise_github_pr("owner/repo", "file.sql", "SELECT 1")

        assert "Error:" in result

    def test_raise_github_pr_returns_string(self, monkeypatch):
        """Return type is always str regardless of outcome."""
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)

        result = raise_github_pr("owner/repo", "file.sql", "SELECT 1")

        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# read_code_file
# ---------------------------------------------------------------------------

class TestReadCodeFile:

    def test_read_existing_file(self, tmp_path, monkeypatch):
        """Reads raw content when the file exists under AIRFLOW_DAGS_DIR."""
        monkeypatch.setenv("AIRFLOW_DAGS_DIR", str(tmp_path))
        model_dir = tmp_path / "dbt_project" / "models"
        model_dir.mkdir(parents=True)
        model_file = model_dir / "stg_customers.sql"
        model_file.write_text("select cust_id as customer_id from raw", encoding="utf-8")

        result = read_code_file("dbt_project/models/stg_customers.sql")

        assert result == "select cust_id as customer_id from raw"

    def test_read_missing_file_returns_error(self, tmp_path, monkeypatch):
        """Missing file returns a descriptive error string, never raises."""
        monkeypatch.setenv("AIRFLOW_DAGS_DIR", str(tmp_path))

        result = read_code_file("dbt_project/models/does_not_exist.sql")

        assert isinstance(result, str)
        assert result.startswith("Error:")
        assert "not found" in result.lower()

    def test_read_forward_slash_path_on_any_os(self, tmp_path, monkeypatch):
        """Forward-slash relative paths resolve correctly regardless of OS."""
        monkeypatch.setenv("AIRFLOW_DAGS_DIR", str(tmp_path))
        f = tmp_path / "a" / "b.sql"
        f.parent.mkdir(parents=True)
        f.write_text("SELECT 1", encoding="utf-8")

        result = read_code_file("a/b.sql")

        assert result == "SELECT 1"


# ---------------------------------------------------------------------------
# execute_test_query (Audit Layer)
# ---------------------------------------------------------------------------

class TestExecuteTestQuery:

    def _set_env(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_USER", "u")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "p")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "myaccount")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "w")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "d")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "s")

    def test_success_returns_rows(self, monkeypatch):
        """A valid query returns a SUCCESS-prefixed string with columns and rows."""
        self._set_env(monkeypatch)

        mock_cursor = MagicMock()
        mock_cursor.description = [("CUSTOMER_ID",), ("EMAIL",)]
        mock_cursor.fetchall.return_value = [
            ("CUST-001", "a@example.com"),
            ("CUST-002", "b@example.com"),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("snowflake.connector.connect", return_value=mock_conn):
            result = execute_test_query(
                "SELECT cust_id AS customer_id, email FROM d.s.RAW_CUSTOMERS LIMIT 5"
            )

        assert result.startswith("SUCCESS:")
        assert "CUSTOMER_ID" in result
        assert "a@example.com" in result
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_compilation_error_returns_exact_message(self, monkeypatch):
        """A ProgrammingError returns the exact Snowflake error so the agent can iterate."""
        import snowflake.connector

        self._set_env(monkeypatch)

        err = snowflake.connector.errors.ProgrammingError(
            msg="SQL compilation error: invalid identifier 'NONEXISTENT_COL'"
        )
        with patch("snowflake.connector.connect", side_effect=err):
            result = execute_test_query("SELECT nonexistent_col FROM d.s.t LIMIT 5")

        assert result.startswith("Error:")
        assert "invalid identifier" in result

    def test_missing_env_returns_error(self, monkeypatch):
        """Missing Snowflake env vars return an error string."""
        for var in ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
                    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"]:
            monkeypatch.delenv(var, raising=False)

        result = execute_test_query("SELECT 1")

        assert result.startswith("Error:")
        assert "Missing" in result

    def test_empty_query_returns_error(self, monkeypatch):
        """An empty query is rejected before connecting."""
        self._set_env(monkeypatch)

        result = execute_test_query("   ")

        assert result.startswith("Error:")

    def test_always_returns_string(self, monkeypatch):
        """Return type is always str, even on unexpected errors."""
        self._set_env(monkeypatch)

        with patch("snowflake.connector.connect", side_effect=Exception("boom")):
            result = execute_test_query("SELECT 1")

        assert isinstance(result, str)
        assert result.startswith("Error:")
