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

import google.generativeai as genai
from dotenv import load_dotenv

from agent.tools import get_airflow_logs, get_snowflake_schema, raise_github_pr

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

    # Gemini model to use — gemini-1.5-flash is cost-efficient and fast enough
    # for agentic orchestration workloads of this kind.
    MODEL_NAME = "gemini-3.8-flash"

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
        genai.configure(api_key=self.api_key)
        logger.info("Gemini API configured with model: %s", self.MODEL_NAME)

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
        # Tool 3: raise_github_pr
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
                    raise_github_pr_declaration,
                ]
            )
        ]

        self.model = genai.GenerativeModel(
            model_name=self.MODEL_NAME,
            tools=tools,
        )
        logger.info("Gemini GenerativeModel initialised with %d tool(s).", len(tools[0].function_declarations))

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
An Airflow pipeline has failed.
DAG ID: {dag_id}
Task ID: {task_id}

Please investigate this failure autonomously:
1. First, retrieve the Airflow task logs to understand what went wrong.
2. If you identify a schema mismatch (column name mismatch between the dbt model
   and Snowflake), query the Snowflake schema to get the actual column names.
3. Generate corrected SQL that aliases the actual Snowflake column to the expected
   name (e.g., cust_id AS customer_id).
4. Raise a GitHub PR with the corrected SQL.

The dbt model file is at: dbt_project/models/stg_customers.sql
Use the GitHub repo: {os.environ.get("GITHUB_REPO", "your-org/data-ops-agent-demo")}
"""

        logger.info(
            "Starting healing workflow for dag_id='%s', task_id='%s'.", dag_id, task_id
        )

        # Start a multi-turn chat session so Gemini can maintain context
        chat = self.model.start_chat()
        response = chat.send_message(initial_prompt)

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

                elif func_name == "get_snowflake_schema":
                    result = get_snowflake_schema(**func_args)
                    # get_snowflake_schema already returns a dict
                    response_data = result

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
            response = chat.send_message(function_responses)

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
