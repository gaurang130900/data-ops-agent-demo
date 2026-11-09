-- =============================================================================
-- SCENARIO 1: Schema / column-name mismatch
-- =============================================================================
-- FAILURE: References c.customer_id, but RAW_CUSTOMERS actually has CUST_ID.
-- Snowflake error: "invalid identifier 'C.CUSTOMER_ID'"
--
-- EXPECTED AGENT FIX: alias cust_id -> customer_id (c.cust_id as customer_id),
-- and reference c.cust_id in the join.
-- =============================================================================

select
    o.order_id,
    c.customer_id,                       -- BUG: column is CUST_ID in RAW_CUSTOMERS
    c.first_name || ' ' || c.last_name as customer_name,
    p.product_name,
    o.quantity
from {{ source('raw', 'raw_orders') }} o
join {{ source('raw', 'raw_customers') }} c
    on o.cust_id = c.customer_id         -- BUG: should be c.cust_id
join {{ source('raw', 'raw_products') }} p
    on o.product_id = p.product_id
