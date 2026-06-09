with source as (
    select * from {{ source('bronze', 'order_reviews') }}
)

select
    review_id,
    order_id,
    review_score::int                    as review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date::timestamp      as review_creation_date,
    review_answer_timestamp::timestamp   as review_answer_timestamp
from source
