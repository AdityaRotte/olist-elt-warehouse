{{ config(severity='warn') }}

-- Per order, total paid should be close to items + freight. Small differences are
-- expected (installment interest, vouchers), so this warns instead of failing.
-- Review the rows it returns rather than treating them as pipeline errors.
with items as (
    select order_id, sum(item_total) as items_total
    from {{ ref('fct_order_items') }}
    group by 1
),

paid as (
    select order_id, sum(payment_value) as paid_total
    from {{ ref('fct_payments') }}
    group by 1
)

select
    i.order_id,
    i.items_total,
    p.paid_total,
    abs(i.items_total - p.paid_total) as difference
from items i
join paid p using (order_id)
where abs(i.items_total - p.paid_total) > 1.00
