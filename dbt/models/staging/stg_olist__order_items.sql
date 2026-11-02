select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }} as order_item_key,
    order_id,
    cast(order_item_id as integer)       as order_item_number,
    product_id,
    seller_id,
    cast(shipping_limit_date as timestamp) as shipping_limit_at,
    cast(price as numeric(12, 2))          as price,
    cast(freight_value as numeric(12, 2))  as freight_value,
    _loaded_at
from {{ source('olist', 'order_items') }}
