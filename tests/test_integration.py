"""
Integration tests for the Autonomous Data Pipeline Healing Agent.

All external dependencies (Airflow API, Snowflake, GitHub, Gemini) are mocked
so these tests run without any live infrastructure.
"""

import pytest
from unittest.mock import patch, MagicMock, call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_function_call_part(name: str, args: dict):
    """Return a mock response Part that looks like a Gemini function call."""
    fc = MagicMock()
    fc.name = name
    fc.args = args

    part = MagicMock()
    part.function_call = fc
    # Make hasattr(part, "function_call") return True
    part.configure_mock(**{"function_call.name": name, "function_call.args": args})
    return part


def _build_text_part(text: str):
    """Return a mock response Part that contains only text (no function call)."""
    part = MagicMock()
    part.function_call = MagicMock()
    part.function_call.name = ""   # empty name signals no function call
    part.text = text
    return part


def _make_response(parts):
    """Wrap parts in a mock Gemini response object."""
    resp = MagicMock()
    resp.parts = parts
    return resp


def _make_genai_stubs():
    """Build minimal genai stubs so TriageAgent.__init__ succeeds."""
    mock_genai = MagicMock()
    mock_genai.protos.FunctionDeclaration = MagicMock(return_value=MagicMock())
    mock_genai.protos.Schema = MagicMock(return_value=MagicMock())
    mock_genai.protos.Type.OBJECT = "OBJECT"
    mock_genai.protos.Type.STRING = "STRING"

    tool_instance = MagicMock()
    tool_instance.function_declarations = [MagicMock(), MagicMock(), MagicMock()]
    mock_genai.protos.Tool.return_value = tool_instance
    mock_genai.protos.Part = MagicMock(return_value=MagicMock())
    mock_genai.protos.FunctionResponse = MagicMock(return_value=MagicMock())
    return mock_genai


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

class TestHealPipelineEndToEnd:

    def _setup_chat(self, mock_genai):
        """
        Wire up a mock chat session that returns function calls in sequence:
          1st response  -> get_airflow_logs
          2nd response  -> get_snowflake_schema
          3rd response  -> raise_github_pr
          4th response  -> text only (done)
        """
        mock_chat = MagicMock()

        resp1 = _make_response([_build_function_call_part(
            "get_airflow_logs",
            {"dag_id": "ingest_to_snowflake", "task_id": "run_stg_customers"},
        )])
        resp2 = _make_response([_build_function_call_part(
            "get_snowflake_schema",
            {"table_name": "DATA_OPS_DEMO.RAW.RAW_CUSTOMERS"},
        )])
        resp3 = _make_response([_build_function_call_part(
            "raise_github_pr",
            {
                "repo_name": "test-org/data-ops-agent-demo",
                "file_path": "dbt_project/models/stg_customers.sql",
                "new_sql": "SELECT cust_id AS customer_id, customer_name, email\nFROM DATA_OPS_DEMO.RAW.RAW_CUSTOMERS",
            },
        )])
        resp4 = _make_response([_build_text_part("Pipeline healed successfully.")])

        mock_chat.send_message.side_effect = [resp1, resp2, resp3, resp4]
        mock_genai.GenerativeModel.return_value.start_chat.return_value = mock_chat
        return mock_chat

    def test_heal_pipeline_end_to_end(self, monkeypatch):
        """Full workflow returns status='success' and the correct PR URL."""
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        monkeypatch.setenv("GITHUB_TOKEN", "test-token")

        mock_genai = _make_genai_stubs()
        self._setup_chat(mock_genai)

        with patch("agent.triage_agent.genai", mock_genai), \
             patch("agent.triage_agent.get_airflow_logs",
                   return_value="Error: column customer_id not found") as mock_logs, \
             patch("agent.triage_agent.get_snowflake_schema",
                   return_value={"columns": [{"name": "CUST_ID", "type": "FIXED"}]}) as mock_schema, \
             patch("agent.triage_agent.raise_github_pr",
                   return_value="https://github.com/test/repo/pull/1") as mock_pr:

            from agent.triage_agent import TriageAgent
            agent = TriageAgent()
            result = agent.heal_pipeline("ingest_to_snowflake", "run_stg_customers")

        assert result["status"] == "success"
        assert result["pr_url"] == "https://github.com/test/repo/pull/1"
        assert len(result["actions_taken"]) == 3

    def test_heal_pipeline_logs_retrieved(self, monkeypatch):
        """get_airflow_logs is called with the correct dag_id and task_id."""
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        monkeypatch.setenv("GITHUB_TOKEN", "test-token")

        mock_genai = _make_genai_stubs()
        self._setup_chat(mock_genai)

        with patch("agent.triage_agent.genai", mock_genai), \
             patch("agent.triage_agent.get_airflow_logs",
                   return_value="some log content") as mock_logs, \
             patch("agent.triage_agent.get_snowflake_schema",
                   return_value={"columns": []}), \
             patch("agent.triage_agent.raise_github_pr",
                   return_value="https://github.com/test/repo/pull/1"):

            from agent.triage_agent import TriageAgent
            agent = TriageAgent()
            agent.heal_pipeline("ingest_to_snowflake", "run_stg_customers")

        mock_logs.assert_called_once_with(
            dag_id="ingest_to_snowflake", task_id="run_stg_customers"
        )

    def test_heal_pipeline_schema_queried(self, monkeypatch):
        """get_snowflake_schema is called during the workflow."""
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        monkeypatch.setenv("GITHUB_TOKEN", "test-token")

        mock_genai = _make_genai_stubs()
        self._setup_chat(mock_genai)

        with patch("agent.triage_agent.genai", mock_genai), \
             patch("agent.triage_agent.get_airflow_logs",
                   return_value="schema mismatch"), \
             patch("agent.triage_agent.get_snowflake_schema",
                   return_value={"columns": [{"name": "CUST_ID", "type": "FIXED"}]}) as mock_schema, \
             patch("agent.triage_agent.raise_github_pr",
                   return_value="https://github.com/test/repo/pull/1"):

            from agent.triage_agent import TriageAgent
            agent = TriageAgent()
            agent.heal_pipeline("ingest_to_snowflake", "run_stg_customers")

        mock_schema.assert_called_once()

    def test_heal_pipeline_pr_created(self, monkeypatch):
        """raise_github_pr is called and its URL appears in the result."""
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        monkeypatch.setenv("GITHUB_TOKEN", "test-token")

        mock_genai = _make_genai_stubs()
        self._setup_chat(mock_genai)

        pr_url = "https://github.com/test/repo/pull/99"

        with patch("agent.triage_agent.genai", mock_genai), \
             patch("agent.triage_agent.get_airflow_logs",
                   return_value="schema mismatch"), \
             patch("agent.triage_agent.get_snowflake_schema",
                   return_value={"columns": []}), \
             patch("agent.triage_agent.raise_github_pr",
                   return_value=pr_url) as mock_pr:

            from agent.triage_agent import TriageAgent
            agent = TriageAgent()
            result = agent.heal_pipeline("ingest_to_snowflake", "run_stg_customers")

        mock_pr.assert_called_once()
        assert result["pr_url"] == pr_url
