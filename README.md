# Autonomous Data Pipeline Healing Agent

An AI-powered agent that automatically detects, diagnoses, and repairs schema mismatches in Airflow-orchestrated dbt pipelines running against Snowflake — then raises a GitHub Pull Request with the fix.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Healing Workflow                         │
│                                                                 │
│  Airflow DAG ──► dbt model ──► Snowflake                       │
│       │                │            │                           │
│       │          schema mismatch    │                           │
│       │          (intentional bug)  │                           │
│       ▼                             ▼                           │
│  TriageAgent (Gemini 1.5-flash)                                │
│       │                                                         │
│       ├─ get_airflow_logs()   ──► Airflow REST API             │
│       ├─ get_snowflake_schema() ─► Snowflake SHOW COLUMNS      │
│       └─ raise_github_pr()   ──► GitHub PR with fixed SQL      │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Location | Purpose |
|---|---|---|
| Airflow DAG | `airflow/dags/ingest_to_snowflake_dag.py` | Triggers dbt model run |
| dbt Model | `dbt_project/models/stg_customers.sql` | Intentionally broken (demo) |
| Tool Functions | `agent/tools.py` | Gemini-callable Python functions |
| Triage Agent | `agent/triage_agent.py` | Gemini orchestrator |
| Unit Tests | `tests/test_tools.py`, `tests/test_triage_agent.py` | pytest unit tests |
| Property Tests | `tests/test_properties.py` | Hypothesis property-based tests |
| Integration Tests | `tests/test_integration.py` | End-to-end mocked workflow tests |

---

## Setup Instructions

### 1. Clone and navigate

```bash
cd data-ops-agent-demo
```

### 2. Create a virtual environment

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Copy `.env` and fill in your credentials:

```bash
# Already exists — just update the placeholder values
```

See the [Environment Variables](#environment-variables) section below.

### 5. Deploy the Airflow DAG

Copy the DAG file to your local Airflow Docker mount:

```bash
copy airflow\dags\ingest_to_snowflake_dag.py C:\Users\gaura\Documents\airflow-local\dags\
```

The DAG will appear automatically in the Airflow UI at http://localhost:8080.

---

## Environment Variables

All credentials are loaded from `.env` in the project root.

| Variable | Required | Description |
|---|---|---|
| `GEMINI_API_KEY` | ✅ | Google Gemini API key |
| `GITHUB_TOKEN` | ✅ | GitHub Personal Access Token (repo scope) |
| `SNOWFLAKE_USER` | ✅ | Snowflake username |
| `SNOWFLAKE_PASSWORD` | ✅ | Snowflake password |
| `SNOWFLAKE_ACCOUNT` | ✅ | Snowflake account identifier (e.g. `xy12345.us-east-1`) |
| `SNOWFLAKE_WAREHOUSE` | ✅ | Snowflake virtual warehouse name |
| `SNOWFLAKE_DATABASE` | ✅ | `DATA_OPS_DEMO` |
| `SNOWFLAKE_SCHEMA` | ✅ | `RAW` |
| `GITHUB_REPO` | optional | Target repo in `owner/repo` format (defaults to prompt value) |

---

## Running the Healing Agent

### Trigger the broken pipeline first

1. Open Airflow UI at http://localhost:8080
2. Enable and trigger the `ingest_to_snowflake` DAG
3. The `run_stg_customers` task will fail (intentional — `customer_id` column does not exist)

### Run the agent

```bash
python -m agent.triage_agent
```

Or from Python:

```python
from agent.triage_agent import TriageAgent

agent = TriageAgent()
result = agent.heal_pipeline(
    dag_id="ingest_to_snowflake",
    task_id="run_stg_customers"
)

print(result["pr_url"])       # GitHub PR URL
print(result["actions_taken"]) # List of tool calls made
print(result["summary"])       # Gemini's final text summary
```

The agent will:
1. Fetch the Airflow task logs via REST API
2. Identify the `customer_id` column mismatch
3. Query Snowflake to confirm the actual column is `cust_id`
4. Generate corrected SQL: `cust_id AS customer_id`
5. Open a GitHub PR with the fix

---

## Running Tests

```bash
# All tests
pytest tests/ -v

# Unit tests only
pytest tests/test_tools.py tests/test_triage_agent.py -v

# Property-based tests (100 examples each)
pytest tests/test_properties.py -v

# Integration tests
pytest tests/test_integration.py -v
```

---

## The Breakage Scenario

The dbt model `dbt_project/models/stg_customers.sql` deliberately references `customer_id`, which does not exist in the Snowflake table `DATA_OPS_DEMO.RAW.RAW_CUSTOMERS`. The actual column name is `cust_id`.

**Broken SQL (initial):**
```sql
SELECT
    customer_id,   -- does not exist in Snowflake
    first_name,
    last_name,
    email,
    created_at
FROM DATA_OPS_DEMO.RAW.RAW_CUSTOMERS
```

**Fixed SQL (agent-generated):**
```sql
SELECT
    cust_id AS customer_id,
    first_name,
    last_name,
    email,
    created_at
FROM DATA_OPS_DEMO.RAW.RAW_CUSTOMERS
```

The agent detects this, generates the fix, and submits it as a PR automatically.
