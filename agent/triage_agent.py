"""
Triage Agent Module for the Autonomous Data Pipeline Healing Agent.

This module implements the TriageAgent class, which uses Google Gemini's native
function-calling capability to autonomously diagnose and repair Airflow/dbt
pipeline failures using a strict six-step Audit Workflow.

Migrated to the modern `google-genai` SDK (>=1.0), which supports Gemini 3.x
models and the "thought signatures" required for multi-turn function calling.

Workflow orchestrated by TriageAgent.heal_pipeline():
  1. DIAGNOSE    - read Airflow/dbt logs to find the failing model and cause.
  2. INSPECT     - read the raw .sql model code.
  3. HYPOTHESIZE - formulate a corrected SQL fix.
  4. AUDIT       - validate the fix against Snowflake (SELECT ... LIMIT 5).
  5. ITERATE     - on audit error, revise and re-test until it passes.
  6. COMMIT      - only after a passing audit, raise a GitHub PR.

Usage::

    agent = TriageAgent()
    result = agent.heal_pipeline("ingest_to_snowflake", "run_stg_customers")
    print(result["pr_url"])
"""

import json
import logging
import os
import time

from google import genai
from google.genai import types
from google.genai import errors as genai_errors
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

    Leverages Gemini's function-calling interface (via the google-genai SDK) to
    coordinate five tools (Airflow logs, Snowflake schema, code reading, the
    Snowflake audit query, and GitHub PR creation) in a closed-loop workflow.
    """

    # Gemini model to use.
    # gemini-3.8-flash hits its 20/day free-tier cap quickly; gemini-3.1-flash-lite
    # has a 500/day limit and is valid for this account (verified via /v1beta/models).
    MODEL_NAME = "gemini-3.1-flash-lite"

    # Max turns in the function-calling loop (prevents runaway execution).
    MAX_ITERATIONS = 12

    # System instruction enforcing the strict, generalized Audit Workflow.
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
        """Load and validate GEMINI_API_KEY and GITHUB_TOKEN from the environment."""
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
        """Create the google-genai client and build the Tool / config objects."""
        # The google-genai client talks to the Gemini Developer API over HTTPS/REST.
        self.client = genai.Client(api_key=self.api_key)
        logger.info("Gemini client initialised with model: %s", self.MODEL_NAME)

        # ------------------------------------------------------------------
        # Tool declarations (one Tool bundling all five function declarations)
        # ------------------------------------------------------------------
        self.tool = types.Tool(
            function_declarations=[
                types.FunctionDeclaration(
                    name="get_airflow_logs",
                    description=(
                        "Retrieves task execution logs from the Airflow REST API for a "
                        "failed DAG task. Use this as the first step when diagnosing any "
                        "Airflow pipeline failure."
                    ),
                    parameters=types.Schema(
                        type=types.Type.OBJECT,
                        properties={
                            "dag_id": types.Schema(
                                type=types.Type.STRING,
                                description="The Airflow DAG identifier (e.g. 'ingest_to_snowflake').",
                            ),
                            "task_id": types.Schema(
                                type=types.Type.STRING,
                                description="The task identifier within the DAG (e.g. 'run_stg_customers').",
                            ),
                        },
                        required=["dag_id", "task_id"],
                    ),
                ),
                types.FunctionDeclaration(
                    name="get_snowflake_schema",
                    description=(
                        "Queries Snowflake to retrieve actual column names and types for a "
                        "given table. Use this after identifying a schema mismatch in the "
                        "Airflow logs to discover the real column names."
                    ),
                    parameters=types.Schema(
                        type=types.Type.OBJECT,
                        properties={
                            "table_name": types.Schema(
                                type=types.Type.STRING,
                                description=(
                                    "Fully qualified table name in DATABASE.SCHEMA.TABLE "
                                    "format (e.g. 'DATA_OPS_DEMO.RAW.RAW_CUSTOMERS')."
                                ),
                            ),
                        },
                        required=["table_name"],
                    ),
                ),
                types.FunctionDeclaration(
                    name="read_code_file",
                    description=(
                        "Reads the raw text content of a source file (e.g. a failing dbt "
                        ".sql model). Use this during the Inspect step to see the ACTUAL "
                        "code of the failing model before formulating a fix. The path is "
                        "relative to the Airflow dags folder "
                        "(e.g. 'dbt_project/models/stg_customers.sql')."
                    ),
                    parameters=types.Schema(
                        type=types.Type.OBJECT,
                        properties={
                            "filepath": types.Schema(
                                type=types.Type.STRING,
                                description=(
                                    "Path to the file relative to the Airflow dags folder "
                                    "(e.g. 'dbt_project/models/stg_customers.sql')."
                                ),
                            ),
                        },
                        required=["filepath"],
                    ),
                ),
                types.FunctionDeclaration(
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
                    parameters=types.Schema(
                        type=types.Type.OBJECT,
                        properties={
                            "query": types.Schema(
                                type=types.Type.STRING,
                                description=(
                                    "The raw Snowflake SQL query to validate "
                                    "(e.g. 'SELECT cust_id AS customer_id, email FROM "
                                    "DATA_OPS_DEMO.RAW.RAW_CUSTOMERS LIMIT 5')."
                                ),
                            ),
                        },
                        required=["query"],
                    ),
                ),
                types.FunctionDeclaration(
                    name="raise_github_pr",
                    description=(
                        "Creates a GitHub branch and raises a Pull Request with corrected "
                        "SQL to fix a pipeline failure. Use this as the final step ONLY "
                        "after execute_test_query has returned a SUCCESS result."
                    ),
                    parameters=types.Schema(
                        type=types.Type.OBJECT,
                        properties={
                            "repo_name": types.Schema(
                                type=types.Type.STRING,
                                description="GitHub repository in 'owner/repo' format.",
                            ),
                            "file_path": types.Schema(
                                type=types.Type.STRING,
                                description=(
                                    "Path to the file within the repository to update "
                                    "(e.g. 'dbt_project/models/stg_customers.sql')."
                                ),
                            ),
                            "new_sql": types.Schema(
                                type=types.Type.STRING,
                                description=(
                                    "Corrected SQL content to write to the file "
                                    "(keep dbt Jinja such as {{ source(...) }})."
                                ),
                            ),
                        },
                        required=["repo_name", "file_path", "new_sql"],
                    ),
                ),
            ]
        )

        # Generation config: register the tool and the system instruction.
        self.generate_config = types.GenerateContentConfig(
            tools=[self.tool],
            system_instruction=self.SYSTEM_INSTRUCTION,
            temperature=0,
        )
        logger.info("Gemini tools configured (5 function declarations).")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _generate_with_retry(self, contents, max_retries: int = 3):
        """Call generate_content with exponential-backoff retry on transient errors."""
        for attempt in range(max_retries):
            try:
                return self.client.models.generate_content(
                    model=self.MODEL_NAME,
                    contents=contents,
                    config=self.generate_config,
                )
            except genai_errors.ServerError as exc:
                # 5xx (incl. 503/504) — retry with backoff.
                if attempt == max_retries - 1:
                    raise
                wait = 10 * (2 ** attempt)
                logger.warning(
                    "Gemini server error (%s), retrying in %ds (attempt %d/%d)...",
                    exc.__class__.__name__, wait, attempt + 1, max_retries,
                )
                time.sleep(wait)

    def _dispatch_tool(self, name: str, args: dict):
        """Execute the named Python tool and return (response_dict, pr_url_or_none)."""
        logger.info("Executing tool: %s(%s)", name, json.dumps(args))
        pr_url = None

        if name == "get_airflow_logs":
            result = get_airflow_logs(**args)
            logger.info(
                "get_airflow_logs returned %d chars. Preview: %s",
                len(result) if isinstance(result, str) else 0,
                (result[:300] if isinstance(result, str) else str(result)),
            )
            return {"result": result}, pr_url

        if name == "get_snowflake_schema":
            return get_snowflake_schema(**args), pr_url

        if name == "read_code_file":
            result = read_code_file(**args)
            logger.info(
                "read_code_file returned %d chars. Preview: %s",
                len(result) if isinstance(result, str) else 0,
                (result[:300] if isinstance(result, str) else str(result)),
            )
            return {"result": result}, pr_url

        if name == "execute_test_query":
            result = execute_test_query(**args)
            audit_ok = isinstance(result, str) and result.startswith("SUCCESS:")
            logger.info(
                "AUDIT execute_test_query -> %s. Preview: %s",
                "PASS" if audit_ok else "FAIL",
                (result[:300] if isinstance(result, str) else str(result)),
            )
            return {"result": result}, pr_url

        if name == "raise_github_pr":
            result = raise_github_pr(**args)
            if isinstance(result, str) and not result.startswith("Error:"):
                pr_url = result
                logger.info("GitHub PR created: %s", pr_url)
            return {"pr_url": result}, pr_url

        logger.warning("Unknown function requested by model: %s", name)
        return {"error": f"Unknown function: {name}"}, pr_url

    # ------------------------------------------------------------------
    # Main orchestration
    # ------------------------------------------------------------------

    def heal_pipeline(self, dag_id: str, task_id: str) -> dict:
        """
        Autonomously investigate a failed Airflow task and raise a verified fix PR.

        Returns a dict with: status, actions_taken, pr_url, summary.
        """
        actions_taken: list[str] = []
        pr_url: str | None = None

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

        # Conversation history as a list of Content turns. The google-genai SDK
        # preserves thought_signatures automatically when we append the model's
        # own Content back into this list — which is what Gemini 3.x requires.
        contents: list[types.Content] = [
            types.Content(role="user", parts=[types.Part(text=initial_prompt)])
        ]

        response = self._generate_with_retry(contents)

        for iteration in range(self.MAX_ITERATIONS):
            logger.info("Function-calling loop iteration %d.", iteration + 1)

            candidate = response.candidates[0] if response.candidates else None
            model_content = candidate.content if candidate else None

            # Collect any function calls from this model turn.
            function_calls = list(response.function_calls or [])

            # Log any thinking/answer text the model produced this turn.
            turn_text = "".join(
                p.text for p in (model_content.parts if model_content and model_content.parts else [])
                if getattr(p, "text", None)
            )
            if turn_text.strip():
                logger.info("Gemini text (iteration %d): %s", iteration + 1, turn_text[:500])

            if not function_calls:
                logger.info("No function calls — workflow complete.")
                break

            # Append the model's turn (with its function calls + thought signatures)
            # to history so the next request echoes them back correctly.
            if model_content is not None:
                contents.append(model_content)

            # Execute each requested tool and build function-response parts.
            tool_response_parts: list[types.Part] = []
            for fc in function_calls:
                args = dict(fc.args) if fc.args else {}
                actions_taken.append(f"Called {fc.name}({args})")

                response_data, maybe_pr = self._dispatch_tool(fc.name, args)
                if maybe_pr:
                    pr_url = maybe_pr

                tool_response_parts.append(
                    types.Part.from_function_response(
                        name=fc.name,
                        response={"result": response_data},
                    )
                )

            # Send all tool results back in a single user turn.
            contents.append(types.Content(role="user", parts=tool_response_parts))

            next_calls = [fc.name for fc in function_calls]
            logger.info("Tools executed in iteration %d: %s", iteration + 1, next_calls)

            response = self._generate_with_retry(contents)

        # Final text summary from the last model response.
        final_text = ""
        if response and response.candidates:
            last = response.candidates[0].content
            if last and last.parts:
                final_text = "".join(p.text for p in last.parts if getattr(p, "text", None))

        status = "success" if pr_url else "completed_without_pr"
        logger.info(
            "Healing workflow finished. status=%s, actions=%d, pr_url=%s",
            status, len(actions_taken), pr_url,
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
