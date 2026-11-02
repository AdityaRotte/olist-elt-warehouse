-- Singular test: returns the rows that violate the rule. Zero rows = pass.
select
    order_item_key,
    order_id,
    purchased_at,
    delivered_to_customer_at
from {{ ref('fct_order_items') }}
where delivered_to_customer_at < purchased_at
