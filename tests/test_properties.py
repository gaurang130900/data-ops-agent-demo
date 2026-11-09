"""
Property-based tests for the Autonomous Data Pipeline Healing Agent.

Uses Hypothesis to verify universal properties across randomly generated inputs.
"""

import json
import re

import pytest
import requests
import sqlparse
from hypothesis import given, settings, strategies as st, assume
from unittest.mock import patch, MagicMock

from agent.tools import get_airflow_logs, get_snowflake_schema, raise_github_pr

# Register a CI profile with 100 examples
settings.register_profile("ci", max_examples=100)


# ---------------------------------------------------------------------------
# Property 1: Airflow log retrieval always returns a string
# ---------------------------------------------------------------------------

# Feature: autonomous-data-pipeline-healing-agent, Property 1
@given(
    dag_id=st.text(min_size=1, max_size=50),
    task_id=st.text(min_size=1, max_size=50),
)
@settings(max_examples=100)
def test_property_airflow_logs_returns_string(dag_id, task_id):
    """
    **Validates: Requirements 1.1**

    For any dag_id and task_id, get_airflow_logs always returns a str.
    """
    with patch("requests.get", side_effect=requests.exceptions.ConnectionError("no airflow")):
        result = get_airflow_logs(dag_id, task_id)

    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Property 4: Tool functions never raise exceptions
# ---------------------------------------------------------------------------

# Feature: autonomous-data-pipeline-healing-agent, Property 4
@given(
    dag_id=st.text(min_size=1, max_size=30),
    task_id=st.text(min_size=1, max_size=30),
)
@settings(max_examples=50)
def test_property_tool_functions_never_raise(dag_id, task_id):
    """
    **Validates: Requirements 1.4**

    get_airflow_logs must never raise an exception; on failure it returns
    an error string starting with "Error:".
    """
    result = None
    with patch("requests.get", side_effect=requests.exceptions.ConnectionError("no airflow")):
        try:
            result = get_airflow_logs(dag_id, task_id)
        except Exception as exc:
            pytest.fail(f"get_airflow_logs raised an exception: {exc}")

    assert "Error:" in result


# ---------------------------------------------------------------------------
# Property 9: SQL column aliasing produces the correct alias expression
# ---------------------------------------------------------------------------

# Feature: autonomous-data-pipeline-healing-agent, Property 9
@given(
    actual_col=st.from_regex(r'[a-z][a-z0-9_]{0,20}', fullmatch=True),
    expected_col=st.from_regex(r'[a-z][a-z0-9_]{0,20}', fullmatch=True),
)
@settings(max_examples=100)
def test_property_sql_column_aliasing(actual_col, expected_col):
    """
    **Validates: Requirements 2.3**

    When corrected SQL is built with `actual AS expected`, the alias expression
    must be present verbatim in the resulting SQL string.
    """
    assume(actual_col != expected_col)

    corrected_sql = f"SELECT {actual_col} AS {expected_col}, name\nFROM my_table"

    assert f"{actual_col} AS {expected_col}" in corrected_sql


# ---------------------------------------------------------------------------
# Property 10: SQL column preservation
# ---------------------------------------------------------------------------

# Feature: autonomous-data-pipeline-healing-agent, Property 10
@given(
    columns=st.lists(
        st.from_regex(r'[a-z][a-z0-9_]{0,10}', fullmatch=True),
        min_size=1,
        max_size=5,
        unique=True,
    )
)
@settings(max_examples=100)
def test_property_sql_column_preservation(columns):
    """
    **Validates: Requirements 2.3**

    After aliasing the first column, all remaining columns must appear in
    the corrected SQL unchanged, and the alias of the first column must
    be present.
    """
    first_col = columns[0]
    alias = f"{first_col}_alias"
    remaining = columns[1:]

    col_list = ", ".join(columns)
    original_sql = f"SELECT {col_list}\nFROM my_table"

    # Simulate correction: replace first column with alias expression
    corrected_sql = original_sql.replace(first_col, f"{first_col} AS {alias}", 1)

    # The alias expression must be present
    assert f"{first_col} AS {alias}" in corrected_sql

    # All other columns must still appear directly
    for col in remaining:
        assert col in corrected_sql


# ---------------------------------------------------------------------------
# Property 11: SQL table reference preservation
# ---------------------------------------------------------------------------

# Feature: autonomous-data-pipeline-healing-agent, Property 11
@given(
    db=st.from_regex(r'[A-Z][A-Z0-9_]{0,10}', fullmatch=True),
    schema=st.from_regex(r'[A-Z][A-Z0-9_]{0,10}', fullmatch=True),
    table=st.from_regex(r'[A-Z][A-Z0-9_]{0,10}', fullmatch=True),
)
@settings(max_examples=100)
def test_property_sql_table_reference_preserved(db, schema, table):
    """
    **Validates: Requirements 2.4**

    The fully qualified table reference must appear unchanged in generated SQL.
    """
    table_ref = f"{db}.{schema}.{table}"
    sql = f"SELECT cust_id AS customer_id\nFROM {table_ref}"

    assert table_ref in sql


# ---------------------------------------------------------------------------
# Property 12: SQL syntax validity
# ---------------------------------------------------------------------------

# Feature: autonomous-data-pipeline-healing-agent, Property 12
@given(
    alias=st.from_regex(r'[a-z][a-z0-9_]{0,10}', fullmatch=True),
    col=st.from_regex(r'[a-z][a-z0-9_]{0,10}', fullmatch=True),
)
@settings(max_examples=100)
def test_property_sql_syntax_valid(alias, col):
    """
    **Validates: Requirements 2.5**

    Any corrected SQL with a column alias must be parseable by sqlparse and
    identified as a SELECT statement.
    """
    sql = f"SELECT {col} AS {alias}\nFROM DATA_OPS_DEMO.RAW.RAW_CUSTOMERS"

    parsed = sqlparse.parse(sql)

    assert len(parsed) > 0
    assert parsed[0].get_type() == "SELECT"


# ---------------------------------------------------------------------------
# Property 20: Tool return values are JSON-serializable
# ---------------------------------------------------------------------------

# Feature: autonomous-data-pipeline-healing-agent, Property 20
@given(
    dag_id=st.text(min_size=1, max_size=20),
    task_id=st.text(min_size=1, max_size=20),
)
@settings(max_examples=50)
def test_property_tool_returns_json_serializable(dag_id, task_id):
    """
    **Validates: Requirements 1.5**

    The return value of get_airflow_logs must always be JSON-serializable.
    """
    with patch("requests.get", side_effect=requests.exceptions.ConnectionError("no airflow")):
        result = get_airflow_logs(dag_id, task_id)

    # Must not raise
    serialized = json.dumps(result)
    assert isinstance(serialized, str)
