select
    product_category_name         as category_name,
    product_category_name_english as category_name_english
from {{ source('olist', 'category_translation') }}
