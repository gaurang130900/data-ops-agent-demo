-- =============================================================================
-- SCENARIO 4: SQL syntax error
-- =============================================================================
-- FAILURE: Trailing comma before FROM and a missing ON keyword in the join.
-- Snowflake error: "SQL compilation error: syntax error line X ..."
--
-- EXPECTED AGENT FIX: remove the trailing comma and restore the JOIN ... ON.
-- =============================================================================

select
    o.order_id,
    c.cust_id as customer_id,
    p.product_name,
    o.quantity,                          -- BUG: trailing comma before FROM
from {{ source('raw', 'raw_orders') }} o
join {{ source('raw', 'raw_customers') }} c
    o.cust_id = c.cust_id                -- BUG: missing ON keyword
join {{ source('raw', 'raw_products') }} p
    on o.product_id = p.product_id
