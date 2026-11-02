{{
    config(
        materialized='incremental',
        unique_key='payment_key',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

-- Grain: one row per payment on an order (an order can be paid with several methods).
with payments as (
    select * from {{ ref('stg_olist__order_payments') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

joined as (
    select
        p.payment_key,
        p.order_id,
        o.customer_id,
        cast(o.purchased_at as date) as purchase_date,
        p.payment_sequential,
        p.payment_type,
        p.installments,
        p.payment_value,
        greatest(p._loaded_at, o._loaded_at) as _loaded_at
    from payments p
    join orders o using (order_id)
)

select * from joined

{% if is_incremental() %}
where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
