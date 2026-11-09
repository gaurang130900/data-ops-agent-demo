"""
Triage Agent Module for the Autonomous Data Pipeline Healing Agent.

This module implements the TriageAgent class, which uses Google Gemini's native
function-calling capability to autonomously diagnose and repair Airflow/dbt
pipeline failures caused by Snowflake schema mismatches.

Workflow orchestrated by TriageAgent.heal_pipeline():
  1. Retrieve Airflow task execution logs to identify the root cause.
  2. Query Snowflake to obtain the actual column names for the affected table.
  3. Generate corrected SQL with appropriate column aliasing.
  4. Raise a GitHub Pull Request containing the fix.

Usage::

    agent = TriageAgent()
    result = agent.heal_pipeline("ingest_to_snowflake", "run_stg_customers")
    print(result["pr_url"])
"""

import json
import logging
import os
import time

import google.generativeai as genai
from google.api_core.exceptions import DeadlineExceeded, ServiceUnavailable
from dotenv import load_dotenv

from agent.tools import (
    get_airflow_logs,
    get_snowflake_schema,
    read_code_file,
    execute_test_query,
    raise_github_pr,
)

# ---------------------------------------------------------------------------
# Module-level logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# TriageAgent
# ---------------------------------------------------------------------------

class TriageAgent:
    """
    AI-powered triage agent that autonomously heals data pipeline failures.

    The agent leverages Google Gemini's function-calling interface to coordinate
    three tool functions (Airflow log retrieval, Snowflake schema introspection,
    and GitHub PR creation) in a closed-loop healing workflow.

    Attributes:
        api_key (str): Gemini API key loaded from the environment.
        github_token (str): GitHub personal access token loaded from the environment.
        model (genai.GenerativeModel): Configured Gemini model with tool definitions.
    """

    # Gemini model to use. This account requires gemini-3.8-flash (the API returns
    # a 404 for gemini-2.5-flash: "no longer available to new users").
    MODEL_NAME = "gemini-3.8-flash"

    # System instruction enforcing the strict, generalized Audit Workflow. This
    # turns the agent into a general-purpose self-healer for any dbt/SQL failure
    # (syntax errors, schema mismatches, invalid casts, etc.) and makes the
    # execute_test_query verification a HARD GATE before any Pull Request.
    SYSTEM_INSTRUCTION = """
You are an autonomous Data SRE (Site Reliability Engineer) agent responsible for
diagnosing and repairing failed Airflow/dbt data pipelines. You are a GENERALIZED
self-healer: you handle ANY failure type (SQL syntax errors, schema/column
mismatches, invalid casts, missing columns, type errors, etc.), not just one kind.

You operate as a strict AUDIT LAYER. You NEVER submit a fix you have not proven.
You MUST follow this exact six-step workflow in order:

1. DIAGNOSE
   - Call get_airflow_logs to read the Airflow/dbt error log.
   - Identify which dbt model (.sql file) failed and the precise reason
     (e.g. syntax error, column/schema mismatch, invalid casting).

2. INSPECT
   - Call read_code_file to fetch the RAW CODE of the failing .sql model.
   - Do not guess the model's contents — always read the actual file.
   - If helpful, call get_snowflake_schema to learn the real column names/types
     of the underlying raw table(s).

3. HYPOTHESIZE
   - Formulate a corrected SQL snippet that fixes the identified error.

4. AUDIT  (CRITICAL — THIS IS MANDATORY)
   - BEFORE modifying any code or raising any PR, you MUST call execute_test_query
     to run your corrected SQL against Snowflake.
   - Use a bounded test: SELECT ... LIMIT 5 against the underlying raw table(s).
   - Resolve any dbt Jinja (e.g. {{ source('raw','raw_customers') }},
     {{ ref(...) }}) into real DATABASE.SCHEMA.TABLE names before testing. The raw
     source table is typically DATA_OPS_DEMO.RAW.RAW_<NAME>.
   - This proves the SQL compiles AND handles the real data correctly.

5. ITERATE
   - If execute_test_query returns a string starting with "Error:", READ the exact
     Snowflake error, adjust your SQL fix accordingly, and call execute_test_query
     AGAIN. Repeat until it returns a result starting with "SUCCESS:".
   - Never give up after a single failed attempt — use the error to learn.

6. COMMIT
   - ONLY after execute_test_query returns "SUCCESS:" are you permitted to call
     raise_github_pr with the fully corrected .sql model content.
   - The new_sql you commit must be the corrected dbt model (keep dbt Jinja such as
     {{ source(...) }} in the committed file — only the AUDIT test query uses the
     resolved table names).

HARD RULES:
- You are FORBIDDEN from calling raise_github_pr before a successful
  execute_test_query result in the current session.
- If after several AUDIT iterations you still cannot get a passing query, stop and
  explain what you tried and the remaining error instead of raising a PR.
- Be concise in your reasoning between tool calls.
"""

    def __init__(self) -> None:
        """Initialize the TriageAgent by loading credentials and configuring Gemini."""
        self.load_environment()
        self.initialize_gemini()

    # ------------------------------------------------------------------
    # Initialization helpers
    # ------------------------------------------------------------------

    def load_environment(self) -> None:
        """
        Load environment variables from the .env file and validate required keys.

        Calls load_dotenv() to populate os.environ from the project's .env file,
        then reads and validates GEMINI_API_KEY and GITHUB_TOKEN.

        Raises:
            ValueError: If GEMINI_API_KEY is absent or empty.
            ValueError: If GITHUB_TOKEN is absent or empty.
        """
        load_dotenv()

        self.api_key = os.environ.get("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "GEMINI_API_KEY environment variable is not set. "
                "Please add it to your .env file before running the agent."
            )

        self.github_token = os.environ.get("GITHUB_TOKEN")
        if not self.github_token:
            raise ValueError(
                "GITHUB_TOKEN environment variable is not set. "
                "Please add it to your .env file before running the agent."
            )

        logger.info("Environment variables loaded successfully.")

    def initialize_gemini(self) -> None:
        """
        Configure the Gemini client and create a GenerativeModel with tool definitions.

        Defines FunctionDeclarations for the three tool functions and registers
        them with the Gemini model so the model can request their invocation
        during the function-calling loop.
        """
        # Force the REST transport. The default gRPC transport can stall on some
        # networks (plain HTTPS/REST works reliably), causing hangs/504s.
        genai.configure(api_key=self.api_key, transport="rest")
        logger.info("Gemini API configured with model: %s (transport=rest)", self.MODEL_NAME)

        # ------------------------------------------------------------------
        # Tool 1: get_airflow_logs
        # ------------------------------------------------------------------
        get_airflow_logs_declaration = genai.protos.FunctionDeclaration(
            name="get_airflow_logs",
            description=(
                "Retrieves task execution logs from the Airflow REST API for a "
                "failed DAG task. Use this as the first step when diagnosing any "
                "Airflow pipeline failure."
            ),
            parameters=genai.protos.Schema(
                type=genai.protos.Type.OBJECT,
                properties={
                    "dag_id": genai.protos.Schema(
                        type=genai.protos.Type.STRING,
                        description="The Airflow DAG identifier (e.g. 'ingest_to_snowflake').",
                    ),
                    "task_id": genai.protos.Schema(
                        type=genai.protos.Type.STRING,
                        description=(
                            "The task identifier within the DAG "
                            "(e.g. 'run_stg_customers')."
                        ),
                    ),
                },
                required=["dag_id", "task_id"],
            ),
        )

        # ------------------------------------------------------------------
        # Tool 2: get_snowflake_schema
        # ------------------------------------------------------------------
        get_snowflake_schema_declaration = genai.protos.FunctionDeclaration(
            name="get_snowflake_schema",
            description=(
                "Queries Snowflake to retrieve actual column names and types for a "
                "given table. Use this after identifying a schema mismatch in the "
                "Airflow logs to discover the real column names."
            ),
            parameters=genai.protos.Schema(
                type=genai.protos.Type.OBJECT,
                properties={
                    "table_name": genai.protos.Schema(
                        type=genai.protos.Type.STRING,
                        description=(
                            "Fully qualified table name in DATABASE.SCHEMA.TABLE "
                            "format (e.g. 'DATA_OPS_DEMO.RAW.RAW_CUSTOMERS')."
                        ),
                    ),
                },
                required=["table_name"],
            ),
        )

        # ------------------------------------------------------------------
        # Tool 3: read_code_file
        # ------------------------------------------------------------------
        read_code_file_declaration = genai.protos.FunctionDeclaration(
            name="read_code_file",
            description=(
                "Reads the raw text content of a source file (e.g. a failing dbt "
                ".sql model). Use this during the Inspect step to see the ACTUAL "
                "code of the failing model before formulating a fix. The path is "
                "relative to the Airflow dags folder "
                "(e.g. 'dbt_project/models/stg_customers.sql')."
            ),
            parameters=genai.protos.Schema(
                type=genai.protos.Type.OBJECT,
                properties={
                    "filepath": genai.protos.Schema(
                        type=genai.protos.Type.STRING,
                        description=(
                            "Path to the file relative to the Airflow dags folder "
                            "(e.g. 'dbt_project/models/stg_customers.sql')."
                        ),
                    ),
                },
                required=["filepath"],
            ),
        )

        # ------------------------------------------------------------------
        # Tool 4: execute_test_query (Audit Layer)
        # ------------------------------------------------------------------
        execute_test_query_declaration = genai.protos.FunctionDeclaration(
            name="execute_test_query",
            description=(
                "AUDIT LAYER. Executes a candidate SQL query against Snowflake and "
                "returns the result rows on success, or the EXACT Snowflake "
                "compilation/execution error on failure. You MUST use this to prove "
                "a proposed SQL fix actually compiles and handles the raw data "
                "correctly BEFORE raising any Pull Request. Use a bounded query such "
                "as 'SELECT ... LIMIT 5' against the underlying raw tables. Resolve "
                "any dbt Jinja (e.g. {{ source(...) }}) to real DATABASE.SCHEMA.TABLE "
                "names before calling. If it returns an error, read the error, revise "
                "your SQL, and call this tool again until it succeeds."
            ),
            parameters=genai.protos.Schema(
                type=genai.protos.Type.OBJECT,
                properties={
                    "query": genai.protos.Schema(
                        type=genai.protos.Type.STRING,
                        description=(
                            "The raw Snowflake SQL query to validate "
                            "(e.g. 'SELECT cust_id AS customer_id, email FROM "
                            "DATA_OPS_DEMO.RAW.RAW_CUSTOMERS LIMIT 5')."
                        ),
                    ),
                },
                required=["query"],
            ),
        )

        # ------------------------------------------------------------------
        # Tool 5: raise_github_pr
        # ------------------------------------------------------------------
        raise_github_pr_declaration = genai.protos.FunctionDeclaration(
            name="raise_github_pr",
            description=(
                "Creates a GitHub branch and raises a Pull Request with corrected "
                "SQL to fix a schema mismatch. Use this as the final step once you "
                "have generated the corrected SQL."
            ),
            parameters=genai.protos.Schema(
                type=genai.protos.Type.OBJECT,
                properties={
                    "repo_name": genai.protos.Schema(
                        type=genai.protos.Type.STRING,
                        description=(
                            "GitHub repository in 'owner/repo' format "
                            "(e.g. 'your-org/data-ops-agent-demo')."
                        ),
                    ),
                    "file_path": genai.protos.Schema(
                        type=genai.protos.Type.STRING,
                        description=(
                            "Path to the file within the repository to update "
                            "(e.g. 'dbt_project/models/stg_customers.sql')."
                        ),
                    ),
                    "new_sql": genai.protos.Schema(
                        type=genai.protos.Type.STRING,
                        description=(
                            "Corrected SQL content to write to the file, with any "
                            "schema mismatch columns aliased appropriately "
                            "(e.g. 'cust_id AS customer_id')."
                        ),
                    ),
                },
                required=["repo_name", "file_path", "new_sql"],
            ),
        )

        # Bundle the declarations into a single Tool object for the model
        tools = [
            genai.protos.Tool(
                function_declarations=[
                    get_airflow_logs_declaration,
                    get_snowflake_schema_declaration,
                    read_code_file_declaration,
                    execute_test_query_declaration,
                    raise_github_pr_declaration,
                ]
            )
        ]

        self.model = genai.GenerativeModel(
            model_name=self.MODEL_NAME,
            tools=tools,
            system_instruction=self.SYSTEM_INSTRUCTION,
        )
        logger.info("Gemini GenerativeModel initialised with %d tool(s).", len(tools[0].function_declarations))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _send_with_retry(chat, message, request_options, max_retries: int = 3):
        """Send a message with exponential-backoff retry on transient 504/503 errors."""
        for attempt in range(max_retries):
            try:
                return chat.send_message(message, request_options=request_options)
            except (DeadlineExceeded, ServiceUnavailable) as exc:
                if attempt == max_retries - 1:
                    raise
                wait = 10 * (2 ** attempt)  # 10s, 20s, 40s
                logger.warning(
                    "Gemini API transient error (%s), retrying in %ds (attempt %d/%d)...",
                    exc.__class__.__name__, wait, attempt + 1, max_retries,
                )
                time.sleep(wait)

    # ------------------------------------------------------------------
    # Main orchestration
    # ------------------------------------------------------------------

    def heal_pipeline(self, dag_id: str, task_id: str) -> dict:
        """
        Autonomously investigate a failed Airflow pipeline task and raise a fix PR.

        Sends an initial diagnostic prompt to Gemini and enters a function-calling
        loop where the model can request the execution of tool functions.  The loop
        runs for a maximum of 10 iterations to prevent runaway execution.

        Args:
            dag_id:  The Airflow DAG identifier that experienced a failure.
            task_id: The task identifier within the DAG that failed.

        Returns:
            A dictionary with the following keys:

            - ``"status"`` (str): ``"success"`` if a PR was raised, otherwise
              ``"completed_without_pr"``.
            - ``"actions_taken"`` (list[str]): Ordered list of tool invocations
              performed during the workflow.
            - ``"pr_url"`` (str | None): The GitHub PR URL, or ``None`` if no PR
              was created.
            - ``"summary"`` (str): The final natural-language summary from Gemini.
        """
        actions_taken: list[str] = []
        pr_url: str | None = None

        # ------------------------------------------------------------------
        # Initial prompt: describe the failure and instruct the agent
        # ------------------------------------------------------------------
        initial_prompt = f"""
An Airflow pipeline has failed and needs autonomous healing.
DAG ID: {dag_id}
Task ID: {task_id}

Follow your six-step Audit Workflow strictly:
  1. DIAGNOSE  - read the Airflow/dbt logs; find the failing .sql model and the cause.
  2. INSPECT   - read the raw code of the failing model with read_code_file.
  3. HYPOTHESIZE - write a corrected SQL fix.
  4. AUDIT     - validate the fix with execute_test_query (SELECT ... LIMIT 5 against
                 the resolved raw table) BEFORE touching any code.
  5. ITERATE   - if the audit query errors, read the error, revise, and re-test until
                 it returns SUCCESS.
  6. COMMIT    - only after a SUCCESS audit, raise a GitHub PR with the corrected model.

Context you may need:
  - The failing dbt model is typically under: dbt_project/models/ (read it to confirm).
  - Raw source tables live in Snowflake as DATA_OPS_DEMO.RAW.RAW_<NAME>.
  - GitHub repo to use for the PR: {os.environ.get("GITHUB_REPO", "your-org/data-ops-agent-demo")}

Do NOT raise a PR until execute_test_query has returned a SUCCESS result.
"""

        logger.info(
            "Starting healing workflow for dag_id='%s', task_id='%s'.", dag_id, task_id
        )

        # Start a multi-turn chat session so Gemini can maintain context
        # request_options sets a generous deadline for agentic multi-tool workflows.
        # google-generativeai 0.8.x accepts a plain dict with a "timeout" key (seconds).
        _request_options = {"timeout": 600}

        chat = self.model.start_chat()
        response = self._send_with_retry(chat, initial_prompt, _request_options)

        # ------------------------------------------------------------------
        # Function-calling loop (max 10 iterations to prevent infinite loops)
        # ------------------------------------------------------------------
        for iteration in range(10):
            logger.info("Function-calling loop iteration %d.", iteration + 1)

            # Collect all function call parts from the current response
            function_calls = []
            for part in response.parts:
                if hasattr(part, "function_call") and part.function_call.name:
                    function_calls.append(part.function_call)

            if not function_calls:
                # Gemini returned a text-only response — the workflow is complete
                logger.info("No function calls in response — workflow complete.")
                break

            # ------------------------------------------------------------------
            # Dispatch each function call to the appropriate tool
            # ------------------------------------------------------------------
            function_responses: list[genai.protos.Part] = []

            for fc in function_calls:
                func_name = fc.name
                func_args = dict(fc.args)

                logger.info("Executing tool: %s(%s)", func_name, json.dumps(func_args))
                actions_taken.append(f"Called {func_name}({func_args})")

                # Route to the correct Python tool function
                if func_name == "get_airflow_logs":
                    result = get_airflow_logs(**func_args)
                    response_data = {"result": result}
                    logger.info(
                        "get_airflow_logs returned %d chars. Preview: %s",
                        len(result) if isinstance(result, str) else 0,
                        (result[:300] if isinstance(result, str) else str(result)),
                    )

                elif func_name == "get_snowflake_schema":
                    result = get_snowflake_schema(**func_args)
                    # get_snowflake_schema already returns a dict
                    response_data = result

                elif func_name == "read_code_file":
                    result = read_code_file(**func_args)
                    response_data = {"result": result}
                    logger.info(
                        "read_code_file returned %d chars. Preview: %s",
                        len(result) if isinstance(result, str) else 0,
                        (result[:300] if isinstance(result, str) else str(result)),
                    )

                elif func_name == "execute_test_query":
                    result = execute_test_query(**func_args)
                    response_data = {"result": result}
                    _audit_ok = isinstance(result, str) and result.startswith("SUCCESS:")
                    logger.info(
                        "AUDIT execute_test_query -> %s. Preview: %s",
                        "PASS" if _audit_ok else "FAIL",
                        (result[:300] if isinstance(result, str) else str(result)),
                    )

                elif func_name == "raise_github_pr":
                    result = raise_github_pr(**func_args)
                    response_data = {"pr_url": result}
                    # Record the PR URL if creation succeeded
                    if isinstance(result, str) and not result.startswith("Error:"):
                        pr_url = result
                        logger.info("GitHub PR created: %s", pr_url)

                else:
                    logger.warning("Unknown function requested by model: %s", func_name)
                    response_data = {"error": f"Unknown function: {func_name}"}

                # Wrap the tool result in a FunctionResponse Part for Gemini
                function_responses.append(
                    genai.protos.Part(
                        function_response=genai.protos.FunctionResponse(
                            name=func_name,
                            response={"result": response_data},
                        )
                    )
                )

            # Send all tool results back to Gemini in a single turn
            response = self._send_with_retry(chat, function_responses, _request_options)

            # Log Gemini's reasoning text (if any) after receiving the tool results
            turn_text = "".join(
                part.text for part in response.parts
                if hasattr(part, "text") and part.text
            )
            if turn_text.strip():
                logger.info("Gemini response text after iteration %d: %s", iteration + 1, turn_text[:500])
            next_calls = [
                part.function_call.name for part in response.parts
                if hasattr(part, "function_call") and part.function_call.name
            ]
            logger.info("Gemini next tool calls after iteration %d: %s", iteration + 1, next_calls or "none")

        # ------------------------------------------------------------------
        # Extract the final text summary from the last Gemini response
        # ------------------------------------------------------------------
        final_text = ""
        for part in response.parts:
            if hasattr(part, "text") and part.text:
                final_text += part.text

        status = "success" if pr_url else "completed_without_pr"
        logger.info(
            "Healing workflow finished. status=%s, actions=%d, pr_url=%s",
            status,
            len(actions_taken),
            pr_url,
        )

        return {
            "status": status,
            "actions_taken": actions_taken,
            "pr_url": pr_url,
            "summary": final_text,
        }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    agent = TriageAgent()
    result = agent.heal_pipeline("ingest_to_snowflake", "run_stg_customers")
    print(json.dumps(result, indent=2))
