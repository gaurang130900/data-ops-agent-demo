-- =============================================================================
-- SCENARIO 3: Non-existent source table / object
-- =============================================================================
-- FAILURE: References raw_order_items, which does not exist. dbt will fail to
-- resolve the source (compilation error), or Snowflake returns:
-- "Object 'DATA_OPS_DEMO.RAW.RAW_ORDER_ITEMS' does not exist or not authorized".
--
-- EXPECTED AGENT FIX: point the join at the correct existing table
-- raw_products (product_id) instead of the fictitious raw_order_items.
-- =============================================================================

select
    o.order_id,
    o.product_id,
    i.product_name,                      -- comes from the wrong/missing table
    o.quantity
from {{ source('raw', 'raw_orders') }} o
join {{ source('raw', 'raw_order_items') }} i   -- BUG: table does not exist
    on o.product_id = i.product_id
