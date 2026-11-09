SELECT
    cust_id AS customer_id,
    first_name,
    last_name,
    email,
    created_at
FROM {{ source('raw', 'raw_customers') }}