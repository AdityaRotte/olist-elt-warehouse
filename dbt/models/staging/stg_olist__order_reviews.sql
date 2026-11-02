-- review_id is NOT unique in the source, so the key is review_id + order_id
select
    {{ dbt_utils.generate_surrogate_key(['review_id', 'order_id']) }} as review_key,
    review_id,
    order_id,
    cast(review_score as integer)            as review_score,
    review_comment_title                     as comment_title,
    review_comment_message                   as comment_message,
    cast(review_creation_date as timestamp)  as created_at,
    cast(review_answer_timestamp as timestamp) as answered_at,
    _loaded_at
from {{ source('olist', 'order_reviews') }}
