"""
Execute 01_create_and_seed.sql against Snowflake to create and seed the test tables.

Usage:
    python snowflake_setup/run_setup.py
"""

import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

HERE = os.path.dirname(os.path.abspath(__file__))
SQL_FILE = os.path.join(HERE, "01_create_and_seed.sql")


def split_statements(sql_text: str) -> list:
    """Split a SQL script into individual statements, stripping comments/blank lines."""
    statements = []
    for raw in sql_text.split(";"):
        # Remove full-line comments and blank lines.
        lines = [
            ln for ln in raw.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        ]
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)
    return statements


def main():
    with open(SQL_FILE, "r", encoding="utf-8") as fh:
        sql_text = fh.read()

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    cur = conn.cursor()
    try:
        for stmt in split_statements(sql_text):
            preview = " ".join(stmt.split())[:80]
            print(f"Executing: {preview} ...")
            cur.execute(stmt)
            if stmt.strip().upper().startswith("SELECT"):
                for row in cur.fetchall():
                    print("   ", row)
        print("\nSetup complete.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
