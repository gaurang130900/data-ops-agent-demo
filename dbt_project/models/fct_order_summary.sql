-- Correct reference model: joins orders -> customers -> products.
-- This is the "golden" version. The files under broken_scenarios/ are
-- intentionally broken copies used to test the healing agent.

select
    o.order_id,
    c.cust_id as customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.email,
    p.product_name,
    p.category,
    o.quantity,
    cast(o.order_amount as number(10,2)) as order_amount,
    to_date(o.order_date, 'YYYY-MM-DD') as order_date
from {{ source('raw', 'raw_orders') }} o
join {{ source('raw', 'raw_customers') }} c
    on o.cust_id = c.cust_id
join {{ source('raw', 'raw_products') }} p
    on o.product_id = p.product_id
