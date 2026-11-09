-- =============================================================================
-- SCENARIO 3: Non-existent source table / object
-- =============================================================================
-- FIXED: Pointed the join at the correct existing table 'raw_products'
-- instead of the fictitious 'raw_order_items'.
-- =============================================================================

select
    o.order_id,
    o.product_id,
    p.product_name,
    o.quantity
from {{ source('raw', 'raw_orders') }} o
join {{ source('raw', 'raw_products') }} p
    on o.product_id = p.product_id
