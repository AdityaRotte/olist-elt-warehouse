select
    p.product_id,
    coalesce(t.category_name_english, p.category_name, 'unknown') as category,
    p.photos_qty,
    p.weight_g,
    p.length_cm * p.height_cm * p.width_cm as volume_cm3
from {{ ref('stg_olist__products') }} p
left join {{ ref('stg_olist__category_translation') }} t
    on p.category_name = t.category_name
