"""
Verify each broken scenario fails against real Snowflake, and each proposed fix
passes — using the agent's own Audit Layer tool (execute_test_query).

This does NOT need dbt or the Gemini API; it resolves the dbt Jinja
{{ source('raw', 'x') }} to DATA_OPS_DEMO.RAW.X and runs the SQL directly.

Usage:
    python snowflake_setup/verify_scenarios.py
"""

import os
import re
import sys

# Ensure the repo root (parent of this script's folder) is importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from agent.tools import execute_test_query  # noqa: E402

RAW_DB_SCHEMA = "DATA_OPS_DEMO.RAW"


def resolve_jinja(sql: str) -> str:
    """Replace {{ source('raw', 'table') }} with DATA_OPS_DEMO.RAW.TABLE."""
    def repl(m):
        table = m.group(2).upper()
        return f"{RAW_DB_SCHEMA}.{table}"
    return re.sub(r"\{\{\s*source\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)\s*\}\}", repl, sql)


def run(label: str, sql: str, expect: str):
    """expect is 'fail' or 'pass'."""
    resolved = resolve_jinja(sql)
    # Wrap as a bounded audit query.
    audit_sql = f"SELECT * FROM ({resolved.rstrip().rstrip(';')}) LIMIT 5"
    result = execute_test_query(audit_sql)
    ok = result.startswith("SUCCESS:") if expect == "pass" else result.startswith("Error:")
    status = "OK " if ok else "XX "
    print(f"[{status}] {label} (expected {expect})")
    print("        ->", result[:180].replace("\n", " "))
    print()
    return ok


# --- Broken scenarios (should FAIL) ---
SCENARIOS_FAIL = {
    "Scenario 1 - schema mismatch": """
        select o.order_id, c.customer_id, p.product_name, o.quantity
        from {{ source('raw', 'raw_orders') }} o
        join {{ source('raw', 'raw_customers') }} c on o.cust_id = c.customer_id
        join {{ source('raw', 'raw_products') }} p on o.product_id = p.product_id
    """,
    "Scenario 2 - invalid cast": """
        select o.order_id, cast(o.order_amount as date) as order_amount
        from {{ source('raw', 'raw_orders') }} o
    """,
    "Scenario 3 - non-existent table": """
        select o.order_id, i.product_name
        from {{ source('raw', 'raw_orders') }} o
        join {{ source('raw', 'raw_order_items') }} i on o.product_id = i.product_id
    """,
    "Scenario 4 - syntax error": """
        select
            o.order_id,
            c.cust_id as customer_id,
            p.product_name,
            o.quantity,
        from {{ source('raw', 'raw_orders') }} o
        join {{ source('raw', 'raw_customers') }} c
            o.cust_id = c.cust_id
        join {{ source('raw', 'raw_products') }} p
            on o.product_id = p.product_id
    """,
}

# --- Corresponding fixes (should PASS) ---
SCENARIOS_PASS = {
    "Fix 1 - schema mismatch": """
        select o.order_id, c.cust_id as customer_id, p.product_name, o.quantity
        from {{ source('raw', 'raw_orders') }} o
        join {{ source('raw', 'raw_customers') }} c on o.cust_id = c.cust_id
        join {{ source('raw', 'raw_products') }} p on o.product_id = p.product_id
    """,
    "Fix 2 - invalid cast": """
        select o.order_id, cast(o.order_amount as number(10,2)) as order_amount,
               to_date(o.order_date, 'YYYY-MM-DD') as order_date
        from {{ source('raw', 'raw_orders') }} o
    """,
    "Fix 3 - correct table": """
        select o.order_id, p.product_name
        from {{ source('raw', 'raw_orders') }} o
        join {{ source('raw', 'raw_products') }} p on o.product_id = p.product_id
    """,
    "Fix 4 - valid syntax": """
        select o.order_id, o.quantity
        from {{ source('raw', 'raw_orders') }} o
    """,
}


if __name__ == "__main__":
    print("=== Broken scenarios (expect FAIL) ===\n")
    for label, sql in SCENARIOS_FAIL.items():
        run(label, sql, "fail")

    print("=== Fixes (expect PASS) ===\n")
    for label, sql in SCENARIOS_PASS.items():
        run(label, sql, "pass")
