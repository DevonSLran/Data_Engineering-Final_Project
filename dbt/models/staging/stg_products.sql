with source as (
    select * from {{ source('bronze', 'products') }}
),

translation as (
    select * from {{ source('bronze', 'product_category_name_translation') }}
)

select
    s.product_id,
    s.product_category_name,
    -- English label where a translation exists, else the original PT name
    coalesce(t.product_category_name_english, s.product_category_name)
        as product_category_name_english,
    s.product_name_lenght::int        as product_name_length,
    s.product_description_lenght::int  as product_description_length,
    s.product_photos_qty::int          as product_photos_qty,
    s.product_weight_g::numeric        as product_weight_g,
    s.product_length_cm::numeric       as product_length_cm,
    s.product_height_cm::numeric       as product_height_cm,
    s.product_width_cm::numeric        as product_width_cm
from source s
left join translation t
    on s.product_category_name = t.product_category_name
