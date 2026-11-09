-- =============================================================================
-- SCENARIO 2: Invalid cast / type error
-- =============================================================================
-- FAILURE: ORDER_DATE is stored as a STRING like '2024-02-01'. Casting it
-- directly with CAST(... AS DATE) using the default format is fine, but here
-- we force an invalid numeric cast of a non-numeric string, AND cast the
-- string date with the wrong assumption.
--
-- The headline bug: casting ORDER_AMOUNT (a STRING) to DATE.
-- Snowflake error: "Date '51.00' is not recognized" / cast failure.
--
-- EXPECTED AGENT FIX: cast order_amount to NUMBER(10,2) and order_date via
-- TO_DATE(order_date, 'YYYY-MM-DD').
-- =============================================================================

select
    o.order_id,
    c.cust_id as customer_id,
    o.quantity,
    cast(o.order_amount as date) as order_amount,   -- BUG: amount is not a date
    cast(o.order_date as number) as order_date       -- BUG: date string -> number
from {{ source('raw', 'raw_orders') }} o
join {{ source('raw', 'raw_customers') }} c
    on o.cust_id = c.cust_id
