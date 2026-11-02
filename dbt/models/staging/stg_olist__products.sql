select
    product_id,
    product_category_name                          as category_name,
    cast(product_name_lenght as integer)           as name_length,        -- sic: typo in source
    cast(product_description_lenght as integer)    as description_length, -- sic: typo in source
    cast(product_photos_qty as integer)            as photos_qty,
    cast(product_weight_g as numeric)              as weight_g,
    cast(product_length_cm as numeric)             as length_cm,
    cast(product_height_cm as numeric)             as height_cm,
    cast(product_width_cm as numeric)              as width_cm
from {{ source('olist', 'products') }}
