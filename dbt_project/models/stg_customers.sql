-- stg_customers.sql
-- Staging model for customer data from Snowflake raw layer.
--
-- NOTE: This model contains an intentional schema mismatch for the
-- autonomous data pipeline healing agent demo. The column `customer_id`
-- referenced below does NOT exist in DATA_OPS_DEMO.RAW.RAW_CUSTOMERS.
-- The actual column name is `cust_id`. This causes the dbt run to fail,
-- allowing the healing agent to detect, diagnose, and auto-fix the issue
-- by raising a GitHub PR with the corrected SQL (cust_id AS customer_id).

SELECT
    customer_id,   -- INTENTIONAL BUG: actual column is cust_id, not customer_id
    first_name,
    last_name,
    email,
    created_at
FROM DATA_OPS_DEMO.RAW.RAW_CUSTOMERS
