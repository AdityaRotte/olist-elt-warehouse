select
    customer_id,           -- per-order id (what the fact table joins on)
    customer_unique_id,    -- the actual person
    city,
    state
from {{ ref('stg_olist__customers') }}
