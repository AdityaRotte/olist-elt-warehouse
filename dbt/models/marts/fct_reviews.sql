{{
    config(
        materialized='incremental',
        unique_key='review_key',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

-- Grain: one row per review of an order.
with reviews as (
    select * from {{ ref('stg_olist__order_reviews') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

joined as (
    select
        r.review_key,
        r.review_id,
        r.order_id,
        o.customer_id,
        cast(o.purchased_at as date)   as purchase_date,
        r.review_score,
        r.comment_message is not null  as has_comment,
        r.created_at                   as review_created_at,
        r.answered_at                  as review_answered_at,
        greatest(r._loaded_at, o._loaded_at) as _loaded_at
    from reviews r
    join orders o using (order_id)
)

select * from joined

{% if is_incremental() %}
where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
