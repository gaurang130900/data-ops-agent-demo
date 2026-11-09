"""
Offline Dry-Run of the Generalized Healing Agent's Audit Workflow.

This harness executes the EXACT six-step Audit sequence the real agent follows,
using the REAL tool functions from agent.tools:

    1. DIAGNOSE    -> get_airflow_logs        (real)
    2. INSPECT     -> read_code_file          (real)
    3. HYPOTHESIZE -> scripted (stands in for Gemini's reasoning)
    4. AUDIT       -> execute_test_query      (real, against live Snowflake)
    5. ITERATE     -> scripted retry on audit failure
    6. COMMIT      -> raise_github_pr         (real) [optional, gated by --commit]

Only the LLM "reasoning" (which fix to try) is scripted. Everything else is the
same code path the production agent uses — so this proves the end-to-end workflow
without consuming the Gemini quota.

Usage:
    python snowflake_setup/dry_run_audit.py              # run audit loop, NO PR
    python snowflake_setup/dry_run_audit.py --commit     # also raise a real PR
"""

import os
import re
import sys

# Make the repo root importable when run from the subfolder.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from agent.tools import (  # noqa: E402
    get_airflow_logs,
    read_code_file,
    execute_test_query,
    raise_github_pr,
)

RAW_DB_SCHEMA = "DATA_OPS_DEMO.RAW"
MODEL_PATH = "dbt_project/models/broken_scenarios/scenario_1_schema_mismatch.sql"
DAG_ID = "ingest_to_snowflake"
TASK_ID = "run_stg_customers"


def banner(step: str, title: str) -> None:
    print("\n" + "=" * 78)
    print(f"  STEP {step}: {title}")
    print("=" * 78)


def resolve_jinja(sql: str) -> str:
    """Resolve {{ source('raw','table') }} -> DATA_OPS_DEMO.RAW.TABLE for the audit."""
    def repl(m):
        return f"{RAW_DB_SCHEMA}.{m.group(2).upper()}"
    return re.sub(
        r"\{\{\s*source\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)\s*\}\}", repl, sql
    )


# ---------------------------------------------------------------------------
# Scripted "Gemini reasoning": the sequence of candidate fixes the model would
# propose. The FIRST attempt is deliberately still wrong, so you can watch the
# AUDIT -> ITERATE loop reject it before the correct fix passes.
# ---------------------------------------------------------------------------
CANDIDATE_FIXES = [
    # Attempt 1 — WRONG on purpose: fixes the SELECT alias but leaves the bad
    # join predicate (c.customer_id) -> audit should still fail.
    """
    select
        o.order_id,
        c.cust_id as customer_id,
        c.first_name || ' ' || c.last_name as customer_name,
        p.product_name,
        o.quantity
    from {{ source('raw', 'raw_orders') }} o
    join {{ source('raw', 'raw_customers') }} c
        on o.cust_id = c.customer_id
    join {{ source('raw', 'raw_products') }} p
        on o.product_id = p.product_id
    """,
    # Attempt 2 — CORRECT: both the SELECT and the join reference cust_id.
    """
    select
        o.order_id,
        c.cust_id as customer_id,
        c.first_name || ' ' || c.last_name as customer_name,
        p.product_name,
        o.quantity
    from {{ source('raw', 'raw_orders') }} o
    join {{ source('raw', 'raw_customers') }} c
        on o.cust_id = c.cust_id
    join {{ source('raw', 'raw_products') }} p
        on o.product_id = p.product_id
    """,
]


def main(commit: bool) -> None:
    # ----- STEP 1: DIAGNOSE -------------------------------------------------
    banner("1", "DIAGNOSE  (real get_airflow_logs)")
    logs = get_airflow_logs(DAG_ID, TASK_ID)
    print(f"Fetched {len(logs)} chars of logs. Tail:")
    print("    " + "\n    ".join(logs.strip().splitlines()[-6:]))

    # ----- STEP 2: INSPECT --------------------------------------------------
    banner("2", "INSPECT  (real read_code_file)")
    model_src = read_code_file(MODEL_PATH)
    if model_src.startswith("Error:"):
        print(model_src)
        return
    print(f"Read failing model ({len(model_src)} chars):")
    print("    " + "\n    ".join(model_src.strip().splitlines()))

    # ----- STEPS 3-5: HYPOTHESIZE -> AUDIT -> ITERATE -----------------------
    banner("3-5", "HYPOTHESIZE -> AUDIT -> ITERATE  (real execute_test_query)")
    passing_fix = None
    for i, candidate in enumerate(CANDIDATE_FIXES, start=1):
        print(f"\n--- Attempt {i}: proposed fix (HYPOTHESIZE) ---")
        resolved = resolve_jinja(candidate)
        audit_sql = f"SELECT * FROM ({resolved.strip().rstrip(';')}) LIMIT 5"
        print("AUDIT query:")
        print("    " + "\n    ".join(audit_sql.strip().splitlines()))

        result = execute_test_query(audit_sql)
        if result.startswith("SUCCESS:"):
            print(f"\n[AUDIT PASS] Attempt {i} validated against Snowflake.")
            print("    " + result[:200])
            passing_fix = candidate
            break
        else:
            print(f"\n[AUDIT FAIL] Attempt {i} rejected by Snowflake (ITERATE):")
            print("    " + result[:200])
            print("    -> reading the error and trying a revised fix...")

    if not passing_fix:
        print("\nNo candidate passed the audit. Per the workflow, NO PR is raised.")
        return

    # ----- STEP 6: COMMIT ---------------------------------------------------
    banner("6", "COMMIT  (real raise_github_pr)" + ("" if commit else "  [SKIPPED — pass --commit to enable]"))
    # The committed file keeps dbt Jinja; only the audit used resolved names.
    committed_sql = passing_fix.strip() + "\n"
    print("Verified fix to commit:")
    print("    " + "\n    ".join(committed_sql.strip().splitlines()))

    if not commit:
        print("\nDry run complete. Audit gate PASSED — a PR WOULD be raised here.")
        print("Re-run with --commit to actually open the Pull Request.")
        return

    repo = os.environ.get("GITHUB_REPO", "your-org/data-ops-agent-demo")
    print(f"\nRaising PR against {repo} ...")
    pr_url = raise_github_pr(
        repo_name=repo,
        file_path=MODEL_PATH,
        new_sql=committed_sql,
    )
    if isinstance(pr_url, str) and pr_url.startswith("Error:"):
        print("PR creation failed:", pr_url)
    else:
        print("Pull Request created:", pr_url)


if __name__ == "__main__":
    main(commit="--commit" in sys.argv)
