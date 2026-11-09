"""
Tool Functions Module for the Autonomous Data Pipeline Healing Agent.

This module exposes five callable tools to the Gemini model via native function calling:
  - get_airflow_logs: Retrieve task execution logs from the Airflow REST API.
  - get_snowflake_schema: Introspect a Snowflake table's column metadata.
  - read_code_file: Read the raw text of a source file (e.g. a failing dbt .sql model).
  - execute_test_query: Audit Layer — run a candidate SQL query against Snowflake and
    return results on success or the exact compilation/execution error on failure.
  - raise_github_pr: Create a GitHub branch and open a Pull Request with corrected SQL.

All functions:
  - Use full type hints compatible with Gemini's function calling interface.
  - Return JSON-serializable values (str or dict).
  - Catch all exceptions internally and return descriptive error messages rather
    than propagating exceptions to the caller.
"""

import json
import logging
import os
import time

import requests
import snowflake.connector
from github import Github, GithubException

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tool 1: get_airflow_logs
# ---------------------------------------------------------------------------

def get_airflow_logs(dag_id: str, task_id: str) -> str:
    """
    Retrieve task execution logs from the Airflow REST API.

    Makes an authenticated HTTP GET request to the local Airflow instance and
    returns the raw log text for the most recent DAG run of the specified task.

    Args:
        dag_id:  The Airflow DAG identifier (e.g. ``"ingest_to_snowflake"``).
        task_id: The task identifier within the DAG (e.g. ``"run_stg_customers"``).

    Returns:
        The task execution log content as a plain string on success.
        A descriptive error string prefixed with ``"Error:"`` on failure.
        This function never raises an exception.

    Example::

        logs = get_airflow_logs("ingest_to_snowflake", "run_stg_customers")
        if logs.startswith("Error:"):
            print("Could not retrieve logs:", logs)
        else:
            print(logs)
    """
    airflow_base_url = "http://localhost:8080"
    logger.info("Fetching Airflow logs: dag_id=%s, task_id=%s", dag_id, task_id)

    try:
        # Airflow 3 uses JWT Bearer token auth
        # Set AIRFLOW_TOKEN in .env (generate from Airflow UI: Admin > API Tokens)
        airflow_token = os.environ.get("AIRFLOW_TOKEN")
        if airflow_token:
            auth_headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {airflow_token}",
            }
            auth_tuple = None
        else:
            # Fallback to Basic Auth
            airflow_user = os.environ.get("AIRFLOW_USER", "airflow")
            airflow_pass = os.environ.get("AIRFLOW_PASSWORD", "airflow")
            auth_headers = {"Accept": "application/json"}
            auth_tuple = (airflow_user, airflow_pass)

        # Step 1: Get the most recent failed dag run ID (fall back to latest if none failed)
        runs_url = (
            f"{airflow_base_url}/api/v2/dags/{dag_id}/dagRuns"
            f"?limit=10&order_by=-start_date"
        )
        runs_resp = requests.get(
            runs_url,
            auth=auth_tuple,
            headers=auth_headers,
            timeout=30,
        )

        if runs_resp.status_code != 200:
            error_msg = (
                f"Error: Could not retrieve DAG runs for dag_id='{dag_id}'. "
                f"HTTP {runs_resp.status_code}. Response: {runs_resp.text[:500]}"
            )
            logger.error(error_msg)
            return error_msg

        dag_runs = runs_resp.json().get("dag_runs", [])
        if not dag_runs:
            error_msg = f"Error: No DAG runs found for dag_id='{dag_id}'."
            logger.error(error_msg)
            return error_msg

        # Prefer the most recent failed run so the agent diagnoses an actual failure
        failed_runs = [r for r in dag_runs if r.get("state") == "failed"]
        selected_run = failed_runs[0] if failed_runs else dag_runs[0]
        run_id = selected_run["dag_run_id"]
        logger.info(
            "Selected dag_run_id: %s (state: %s)", run_id, selected_run.get("state")
        )

        # Step 2: Get task logs for that run
        log_url = (
            f"{airflow_base_url}/api/v2/dags/{dag_id}"
            f"/dagRuns/{run_id}/taskInstances/{task_id}/logs/1"
        )
        log_resp = requests.get(
            log_url,
            auth=auth_tuple,
            headers=auth_headers,
            timeout=30,
        )

        if log_resp.status_code == 200:
            try:
                data = log_resp.json()
                content = data.get("content") if isinstance(data, dict) else data
                # Airflow v3: content is a list of {"event": "..."} objects
                # Airflow v2: content is a plain string
                if isinstance(content, list):
                    lines = [
                        entry.get("event", "")
                        for entry in content
                        if isinstance(entry, dict) and entry.get("event")
                    ]
                    log_content = "\n".join(lines) if lines else log_resp.text
                elif isinstance(content, str):
                    log_content = content
                else:
                    log_content = log_resp.text
            except ValueError:
                log_content = log_resp.text
            logger.info(
                "Successfully retrieved logs for dag_id=%s task_id=%s (%d chars)",
                dag_id, task_id, len(log_content),
            )
            return log_content
        else:
            error_msg = (
                f"Error: Airflow API returned HTTP {log_resp.status_code} "
                f"for dag_id='{dag_id}', task_id='{task_id}'. "
                f"Response body: {log_resp.text[:500]}"
            )
            logger.error(error_msg)
            return error_msg

    except requests.exceptions.ConnectionError as exc:
        error_msg = (
            f"Error: Unable to connect to Airflow API at {airflow_base_url}. "
            f"Ensure Airflow is running and accessible. Details: {exc}"
        )
        logger.error(error_msg)
        return error_msg

    except requests.exceptions.Timeout as exc:
        error_msg = (
            f"Error: Request to Airflow API timed out after 30 seconds "
            f"for dag_id='{dag_id}', task_id='{task_id}'. Details: {exc}"
        )
        logger.error(error_msg)
        return error_msg

    except Exception as exc:  # noqa: BLE001
        error_msg = (
            f"Error: Unexpected error while retrieving Airflow logs for "
            f"dag_id='{dag_id}', task_id='{task_id}'. Details: {exc}"
        )
        logger.exception(error_msg)
        return error_msg


# ---------------------------------------------------------------------------
# Tool 2: get_snowflake_schema
# ---------------------------------------------------------------------------

def get_snowflake_schema(table_name: str) -> dict:
    """
    Query Snowflake to retrieve column metadata for the specified table.

    Uses the official snowflake-connector-python library with SHOW COLUMNS command.

    Environment variables required:
        ``SNOWFLAKE_USER``, ``SNOWFLAKE_PASSWORD``, ``SNOWFLAKE_ACCOUNT``,
        ``SNOWFLAKE_WAREHOUSE``, ``SNOWFLAKE_DATABASE``, ``SNOWFLAKE_SCHEMA``

    Args:
        table_name: Fully qualified table name in the format
                    ``DATABASE.SCHEMA.TABLE`` (e.g.
                    ``"DATA_OPS_DEMO.RAW.RAW_CUSTOMERS"``).

    Returns:
        On success: ``{"columns": [{"name": "CUST_ID", "type": "TEXT"}, ...]}``
        On failure: ``{"error": "<descriptive error message>"}``
        Never raises an exception.
    """
    logger.info("Fetching Snowflake schema for table: %s", table_name)

    snowflake_user      = os.environ.get("SNOWFLAKE_USER")
    snowflake_password  = os.environ.get("SNOWFLAKE_PASSWORD")
    snowflake_account   = os.environ.get("SNOWFLAKE_ACCOUNT")
    snowflake_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    snowflake_database  = os.environ.get("SNOWFLAKE_DATABASE")
    snowflake_schema    = os.environ.get("SNOWFLAKE_SCHEMA")

    missing = [
        name for name, val in {
            "SNOWFLAKE_USER":      snowflake_user,
            "SNOWFLAKE_PASSWORD":  snowflake_password,
            "SNOWFLAKE_ACCOUNT":   snowflake_account,
            "SNOWFLAKE_WAREHOUSE": snowflake_warehouse,
            "SNOWFLAKE_DATABASE":  snowflake_database,
            "SNOWFLAKE_SCHEMA":    snowflake_schema,
        }.items() if not val
    ]
    if missing:
        error_msg = (
            f"Error: Missing Snowflake environment variables: {', '.join(missing)}. "
            "Please configure them in your .env file."
        )
        logger.error(error_msg)
        return {"error": error_msg}

    conn = None
    cursor = None
    try:
        # Parse the fully qualified table name
        parts = table_name.upper().split(".")
        if len(parts) == 3:
            db, schema, tbl = parts
        elif len(parts) == 2:
            db = snowflake_database.upper()
            schema, tbl = parts
        else:
            db = snowflake_database.upper()
            schema = snowflake_schema.upper()
            tbl = parts[0]

        # Connect using snowflake.connector
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            database=db,
            schema=schema,
        )
        logger.info("Snowflake connection established successfully.")

        # Create cursor and execute SHOW COLUMNS command
        cursor = conn.cursor()
        qualified_table_name = f"{db}.{schema}.{tbl}"
        show_columns_sql = f"SHOW COLUMNS IN TABLE {qualified_table_name}"
        cursor.execute(show_columns_sql)
        
        # Fetch all results
        rows = cursor.fetchall()
        
        # Parse column metadata from SHOW COLUMNS output
        # SHOW COLUMNS returns: [table_name, schema_name, column_name, data_type, ...]
        # Column name is at index 2, data type is at index 3
        columns = [
            {"name": row[2], "type": row[3]}
            for row in rows if len(row) > 3
        ]

        logger.info("Retrieved %d column(s) for table %s", len(columns), table_name)
        return {"columns": columns}

    except snowflake.connector.errors.ProgrammingError as exc:
        error_msg = f"Error: Snowflake query failed for table '{table_name}'. Details: {exc}"
        logger.error(error_msg)
        return {"error": error_msg}
    except snowflake.connector.errors.DatabaseError as exc:
        error_msg = f"Error: Snowflake database error for table '{table_name}'. Details: {exc}"
        logger.error(error_msg)
        return {"error": error_msg}
    except snowflake.connector.errors.Error as exc:
        error_msg = f"Error: Cannot connect to Snowflake. Check SNOWFLAKE_ACCOUNT and credentials. Details: {exc}"
        logger.error(error_msg)
        return {"error": error_msg}
    except Exception as exc:  # noqa: BLE001
        error_msg = f"Error: Unexpected error querying Snowflake schema for '{table_name}'. Details: {exc}"
        logger.exception(error_msg)
        return {"error": error_msg}
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ---------------------------------------------------------------------------
# Tool 3: read_code_file
# ---------------------------------------------------------------------------

def _resolve_dags_base_dirs() -> list:
    """
    Build an ordered list of candidate base directories for resolving code files.

    Resolution order:
      1. The ``AIRFLOW_DAGS_DIR`` environment variable, if set.
      2. The local repository's dbt project parent (``<repo>/``), so paths like
         ``dbt_project/models/stg_customers.sql`` resolve inside the repo.
      3. The mirrored host Airflow dags directory used by the Docker container.

    Returns:
        A list of absolute base directory paths to try, in priority order.
    """
    candidates = []

    env_dir = os.environ.get("AIRFLOW_DAGS_DIR")
    if env_dir:
        candidates.append(env_dir)

    # Local repo root: this file is at <repo>/agent/tools.py, so repo root is one up.
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidates.append(repo_root)

    # Mirrored host Airflow dags directory (Docker bind mount source).
    candidates.append(r"C:\Users\gaura\Documents\airflow-local\dags")

    return candidates


def read_code_file(filepath: str) -> str:
    """
    Read and return the raw text content of a source file.

    Intended for the agent to inspect the actual code of a failing dbt ``.sql``
    model (or any other source file) before formulating a fix. The ``filepath``
    is interpreted relative to the Airflow dags folder / project root. Several
    candidate base directories are tried so the tool works both against the local
    repository and the mirrored Airflow dags directory.

    Args:
        filepath: Path to the file, relative to the Airflow dags folder / repo
                  root (e.g. ``"dbt_project/models/stg_customers.sql"``). An
                  absolute path is also accepted and used as-is.

    Returns:
        The raw file contents as a string on success.
        A descriptive error string prefixed with ``"Error:"`` if the file cannot
        be found or read. This function never raises an exception.
    """
    logger.info("Reading code file: %s", filepath)

    # Normalize separators so forward-slash paths work on Windows too.
    normalized = filepath.replace("/", os.sep).replace("\\", os.sep).lstrip(os.sep)

    # If an absolute path was provided, try it directly first.
    tried: list = []
    search_paths: list = []
    if os.path.isabs(filepath):
        search_paths.append(filepath)
    for base in _resolve_dags_base_dirs():
        search_paths.append(os.path.join(base, normalized))

    for candidate in search_paths:
        tried.append(candidate)
        if os.path.isfile(candidate):
            try:
                with open(candidate, "r", encoding="utf-8") as fh:
                    content = fh.read()
                logger.info("Read %d chars from %s", len(content), candidate)
                return content
            except OSError as exc:
                error_msg = f"Error: Could not read file '{candidate}'. Details: {exc}"
                logger.error(error_msg)
                return error_msg

    error_msg = (
        f"Error: File not found: '{filepath}'. Searched the following locations: "
        f"{'; '.join(tried)}. Provide a path relative to the Airflow dags folder "
        f"(e.g. 'dbt_project/models/stg_customers.sql')."
    )
    logger.error(error_msg)
    return error_msg


# ---------------------------------------------------------------------------
# Tool 4: execute_test_query (Audit Layer)
# ---------------------------------------------------------------------------

def execute_test_query(query: str) -> str:
    """
    Audit Layer: execute a candidate SQL query against Snowflake and report the outcome.

    This is the verification gate the agent MUST use to prove a proposed SQL fix
    actually compiles and runs against the real data before any Pull Request is
    raised. Use a bounded query such as ``SELECT ... LIMIT 5`` against the
    underlying raw tables.

    Uses the official snowflake-connector-python library. Connection parameters
    are read from the environment:
        ``SNOWFLAKE_USER``, ``SNOWFLAKE_PASSWORD``, ``SNOWFLAKE_ACCOUNT``,
        ``SNOWFLAKE_WAREHOUSE``, ``SNOWFLAKE_DATABASE``, ``SNOWFLAKE_SCHEMA``

    Args:
        query: The raw SQL query to execute. Must be valid Snowflake SQL (resolve
               any dbt Jinja such as ``{{ source(...) }}`` to real
               ``DATABASE.SCHEMA.TABLE`` names before calling).

    Returns:
        On success: a string containing the column headers and up to the returned
        rows (JSON-serialized), prefixed with ``"SUCCESS:"``.
        On failure: the exact Snowflake compilation/execution error message,
        prefixed with ``"Error:"``, so the agent can read it and revise the fix.
        This function never raises an exception.
    """
    logger.info("Audit Layer executing test query: %s", query[:300])

    snowflake_user      = os.environ.get("SNOWFLAKE_USER")
    snowflake_password  = os.environ.get("SNOWFLAKE_PASSWORD")
    snowflake_account   = os.environ.get("SNOWFLAKE_ACCOUNT")
    snowflake_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    snowflake_database  = os.environ.get("SNOWFLAKE_DATABASE")
    snowflake_schema    = os.environ.get("SNOWFLAKE_SCHEMA")

    missing = [
        name for name, val in {
            "SNOWFLAKE_USER":      snowflake_user,
            "SNOWFLAKE_PASSWORD":  snowflake_password,
            "SNOWFLAKE_ACCOUNT":   snowflake_account,
            "SNOWFLAKE_WAREHOUSE": snowflake_warehouse,
            "SNOWFLAKE_DATABASE":  snowflake_database,
            "SNOWFLAKE_SCHEMA":    snowflake_schema,
        }.items() if not val
    ]
    if missing:
        error_msg = (
            f"Error: Missing Snowflake environment variables: {', '.join(missing)}. "
            "Please configure them in your .env file."
        )
        logger.error(error_msg)
        return error_msg

    if not query or not query.strip():
        return "Error: Empty query provided to execute_test_query."

    conn = None
    cursor = None
    try:
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema,
        )
        cursor = conn.cursor()
        cursor.execute(query)

        columns = [col[0] for col in cursor.description] if cursor.description else []
        rows = cursor.fetchall()

        # Serialize rows defensively (handle dates/decimals/etc. via str fallback).
        serializable_rows = [
            [col if isinstance(col, (str, int, float, bool, type(None))) else str(col)
             for col in row]
            for row in rows
        ]

        result = {
            "columns": columns,
            "row_count": len(serializable_rows),
            "rows": serializable_rows,
        }
        logger.info(
            "Audit query succeeded: %d column(s), %d row(s).",
            len(columns), len(serializable_rows),
        )
        return "SUCCESS: " + json.dumps(result, default=str)

    except snowflake.connector.errors.ProgrammingError as exc:
        # Compilation / SQL errors land here — return the exact message to the agent.
        error_msg = f"Error: Snowflake rejected the query. {exc}"
        logger.error(error_msg)
        return error_msg
    except snowflake.connector.errors.Error as exc:
        error_msg = f"Error: Snowflake execution error. {exc}"
        logger.error(error_msg)
        return error_msg
    except Exception as exc:  # noqa: BLE001
        error_msg = f"Error: Unexpected error executing test query. Details: {exc}"
        logger.exception(error_msg)
        return error_msg
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ---------------------------------------------------------------------------
# Tool 5: raise_github_pr
# ---------------------------------------------------------------------------

def raise_github_pr(repo_name: str, file_path: str, new_sql: str) -> str:
    """
    Create a GitHub branch and open a Pull Request with corrected SQL.

    Authenticates with the GitHub API using the ``GITHUB_TOKEN`` environment
    variable, creates a timestamped branch from ``main``, updates the specified
    file with the corrected SQL, and opens a Pull Request describing the
    schema-mismatch fix.

    Environment variables required:
        ``GITHUB_TOKEN``

    Args:
        repo_name: Repository in ``"owner/repo"`` format
                   (e.g. ``"my-org/data-ops-agent-demo"``).
        file_path: Path to the file to update within the repository
                   (e.g. ``"dbt_project/models/stg_customers.sql"``).
        new_sql:   Corrected SQL content to write to the file.

    Returns:
        The HTML URL of the created Pull Request on success
        (e.g. ``"https://github.com/owner/repo/pull/42"``).
        A descriptive error string prefixed with ``"Error:"`` on failure.
        This function never raises an exception.

    Example::

        pr_url = raise_github_pr(
            "my-org/data-ops-agent-demo",
            "dbt_project/models/stg_customers.sql",
            "SELECT cust_id AS customer_id, customer_name, email\\n"
            "FROM DATA_OPS_DEMO.RAW.RAW_CUSTOMERS",
        )
        if pr_url.startswith("Error:"):
            print("PR creation failed:", pr_url)
        else:
            print("Pull Request created:", pr_url)
    """
    logger.info(
        "Creating GitHub PR: repo=%s, file_path=%s", repo_name, file_path
    )

    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        error_msg = (
            "Error: GITHUB_TOKEN environment variable is not set. "
            "Please add it to your .env file before running the agent."
        )
        logger.error(error_msg)
        return error_msg

    try:
        g = Github(github_token)

        # Verify authentication by accessing the authenticated user
        try:
            authenticated_user = g.get_user().login
            logger.info("Authenticated to GitHub as: %s", authenticated_user)
        except GithubException as exc:
            error_msg = (
                f"Error: GitHub authentication failed. "
                f"Verify that GITHUB_TOKEN is valid and not expired. "
                f"Details: {exc}"
            )
            logger.error(error_msg)
            return error_msg

        # Get the target repository
        try:
            repo = g.get_repo(repo_name)
        except GithubException as exc:
            error_msg = (
                f"Error: Repository '{repo_name}' not found or not accessible "
                f"with the provided GITHUB_TOKEN. Details: {exc}"
            )
            logger.error(error_msg)
            return error_msg

        # Determine the SHA of the tip of the main branch
        try:
            main_branch = repo.get_branch("main")
            main_sha = main_branch.commit.sha
            logger.info("main branch SHA: %s", main_sha)
        except GithubException as exc:
            error_msg = (
                f"Error: Could not retrieve 'main' branch from repository "
                f"'{repo_name}'. Ensure the branch exists. Details: {exc}"
            )
            logger.error(error_msg)
            return error_msg

        # Create a new branch from main
        branch_name = f"fix/schema-mismatch-{int(time.time())}"
        try:
            repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=main_sha)
            logger.info("Created branch: %s", branch_name)
        except GithubException as exc:
            error_msg = (
                f"Error: Failed to create branch '{branch_name}' in repository "
                f"'{repo_name}'. Details: {exc}"
            )
            logger.error(error_msg)
            return error_msg

        # Retrieve the current file to get its blob SHA (required for update)
        try:
            current_file = repo.get_contents(file_path, ref="main")
            file_sha = current_file.sha
            logger.info("Retrieved current file '%s' (sha=%s)", file_path, file_sha)
        except GithubException as exc:
            error_msg = (
                f"Error: Could not retrieve file '{file_path}' from repository "
                f"'{repo_name}' on branch 'main'. Details: {exc}"
            )
            logger.error(error_msg)
            return error_msg

        # Commit the updated file to the new branch
        commit_message = (
            f"Fix schema mismatch: rename customer_id reference to "
            f"cust_id AS customer_id in {file_path}"
        )
        try:
            repo.update_file(
                path=file_path,
                message=commit_message,
                content=new_sql,
                sha=file_sha,
                branch=branch_name,
            )
            logger.info("Updated file '%s' on branch '%s'", file_path, branch_name)
        except GithubException as exc:
            error_msg = (
                f"Error: Failed to update file '{file_path}' on branch "
                f"'{branch_name}'. Details: {exc}"
            )
            logger.error(error_msg)
            return error_msg

        # Open the Pull Request
        pr_title = "Fix: Schema mismatch in stg_customers.sql"
        pr_body = (
            "## Problem\n\n"
            "The dbt model `stg_customers.sql` references a column named "
            "`customer_id`, which does not exist in the Snowflake source table "
            "`DATA_OPS_DEMO.RAW.RAW_CUSTOMERS`. The actual column name in "
            "Snowflake is `cust_id`, causing the Airflow pipeline to fail with a "
            "schema mismatch error.\n\n"
            "## Root Cause\n\n"
            "| Location | Column Name |\n"
            "|---|---|\n"
            "| dbt model (`stg_customers.sql`) | `customer_id` |\n"
            "| Snowflake (`RAW_CUSTOMERS`) | `cust_id` |\n\n"
            "## Fix\n\n"
            "The corrected SQL aliases the Snowflake column to preserve the "
            "expected downstream column name:\n\n"
            "```sql\n"
            "SELECT\n"
            "    cust_id AS customer_id,\n"
            "    first_name,\n"
            "    last_name,\n"
            "    email,\n"
            "    created_at\n"
            "FROM DATA_OPS_DEMO.RAW.RAW_CUSTOMERS\n"
            "```\n\n"
            "## How This Was Detected\n\n"
            "This PR was automatically generated by the **Autonomous Data Pipeline "
            "Healing Agent** after detecting the schema mismatch in the Airflow "
            "task logs and confirming the actual schema via Snowflake introspection.\n"
        )

        try:
            pr = repo.create_pull(
                title=pr_title,
                body=pr_body,
                head=branch_name,
                base="main",
            )
            logger.info("Pull Request created: %s", pr.html_url)
            return pr.html_url

        except GithubException as exc:
            error_msg = (
                f"Error: Failed to create Pull Request from branch '{branch_name}' "
                f"to 'main' in repository '{repo_name}'. Details: {exc}"
            )
            logger.error(error_msg)
            return error_msg

    except Exception as exc:  # noqa: BLE001
        error_msg = (
            f"Error: Unexpected error while creating GitHub PR for repository "
            f"'{repo_name}', file '{file_path}'. Details: {exc}"
        )
        logger.exception(error_msg)
        return error_msg
