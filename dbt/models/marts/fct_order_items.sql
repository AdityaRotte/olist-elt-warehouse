{{
    config(
        materialized='incremental',
        unique_key='order_item_key',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

-- Grain: one row per order item.
with items as (
    select * from {{ ref('stg_olist__order_items') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

joined as (
    select
        i.order_item_key,
        i.order_id,
        i.order_item_number,
        o.customer_id,
        i.product_id,
        i.seller_id,
        cast(o.purchased_at as date)  as purchase_date,
        o.order_status,
        i.price,
        i.freight_value,
        i.price + i.freight_value     as item_total,
        o.purchased_at,
        o.delivered_to_customer_at,
        o.estimated_delivery_at,
        extract(epoch from (o.delivered_to_customer_at - o.purchased_at)) / 86400.0
                                      as delivery_days,
        o.delivered_to_customer_at > o.estimated_delivery_at as is_late,
        greatest(i._loaded_at, o._loaded_at) as _loaded_at
    from items i
    join orders o using (order_id)
)

select * from joined

{% if is_incremental() %}
-- only rows that arrived since the last run
where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
