select
    customer_id,
    customer_unique_id,          -- the real person; customer_id is per order
    customer_zip_code_prefix as zip_code_prefix,
    customer_city            as city,
    customer_state           as state
from {{ source('olist', 'customers') }}
