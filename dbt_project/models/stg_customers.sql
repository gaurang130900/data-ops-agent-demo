select
    cust_id as customer_id,
    first_name,
    last_name,
    email,
    created_at
from {{ source('raw', 'raw_customers') }}
