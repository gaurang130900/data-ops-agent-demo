"""
Unit tests for agent/triage_agent.py (TriageAgent class).

All tests patch google.generativeai to avoid real API calls.
"""

import pytest
from unittest.mock import patch, MagicMock, call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_genai_mocks():
    """Return a pair (mock_genai_module, mock_model) for patching."""
    mock_model = MagicMock()
    mock_genai = MagicMock()
    mock_genai.GenerativeModel.return_value = mock_model

    # Build realistic protos stubs so TriageAgent.__init__ doesn't crash
    mock_genai.protos.FunctionDeclaration = MagicMock(return_value=MagicMock())
    mock_genai.protos.Schema = MagicMock(return_value=MagicMock())
    mock_genai.protos.Type.OBJECT = "OBJECT"
    mock_genai.protos.Type.STRING = "STRING"
    mock_genai.protos.Tool = MagicMock(return_value=MagicMock())

    # Make the Tool object return a list of 3 function declarations
    tool_instance = MagicMock()
    tool_instance.function_declarations = [MagicMock(), MagicMock(), MagicMock()]
    mock_genai.protos.Tool.return_value = tool_instance

    return mock_genai, mock_model


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestTriageAgentEnvironment:

    def test_load_environment_success(self, monkeypatch):
        """TriageAgent initialises without error when required env vars are set."""
        monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
        monkeypatch.setenv("GITHUB_TOKEN", "test-github-token")

        mock_genai, _ = _make_genai_mocks()

        with patch("agent.triage_agent.genai", mock_genai):
            from agent.triage_agent import TriageAgent
            agent = TriageAgent()

        assert agent is not None

    def test_load_environment_missing_gemini_key(self, monkeypatch):
        """ValueError is raised when GEMINI_API_KEY is absent."""
        monkeypatch.delenv("GEMINI_API_KEY", raising=False)
        monkeypatch.setenv("GITHUB_TOKEN", "test-github-token")

        mock_genai, _ = _make_genai_mocks()

        with patch("agent.triage_agent.genai", mock_genai):
            from agent.triage_agent import TriageAgent
            with pytest.raises(ValueError, match="GEMINI_API_KEY"):
                TriageAgent()

    def test_load_environment_missing_github_token(self, monkeypatch):
        """ValueError is raised when GITHUB_TOKEN is absent."""
        monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)

        mock_genai, _ = _make_genai_mocks()

        with patch("agent.triage_agent.genai", mock_genai):
            from agent.triage_agent import TriageAgent
            with pytest.raises(ValueError, match="GITHUB_TOKEN"):
                TriageAgent()

    def test_initialize_gemini_called(self, monkeypatch):
        """genai.configure is called with the correct api_key."""
        api_key = "my-unique-api-key-xyz"
        monkeypatch.setenv("GEMINI_API_KEY", api_key)
        monkeypatch.setenv("GITHUB_TOKEN", "test-github-token")

        mock_genai, _ = _make_genai_mocks()

        with patch("agent.triage_agent.genai", mock_genai):
            from agent.triage_agent import TriageAgent
            TriageAgent()

        mock_genai.configure.assert_called_once_with(api_key=api_key)

    def test_tool_registration(self, monkeypatch):
        """The GenerativeModel is created with a tools list containing 3 function declarations."""
        monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
        monkeypatch.setenv("GITHUB_TOKEN", "test-github-token")

        mock_genai, mock_model = _make_genai_mocks()

        with patch("agent.triage_agent.genai", mock_genai):
            from agent.triage_agent import TriageAgent
            agent = TriageAgent()

        # Verify GenerativeModel was called with a tools kwarg
        _, kwargs = mock_genai.GenerativeModel.call_args
        tools_arg = kwargs.get("tools", [])
        assert len(tools_arg) >= 1

        # The single Tool object should carry 3 function declarations
        tool_obj = tools_arg[0]
        assert len(tool_obj.function_declarations) == 3
