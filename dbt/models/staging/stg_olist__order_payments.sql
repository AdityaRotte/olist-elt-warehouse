select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'payment_sequential']) }} as payment_key,
    order_id,
    cast(payment_sequential as integer)   as payment_sequential,
    payment_type,
    cast(payment_installments as integer) as installments,
    cast(payment_value as numeric(12, 2)) as payment_value,
    _loaded_at
from {{ source('olist', 'order_payments') }}
