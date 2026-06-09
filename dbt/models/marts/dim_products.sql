-- Product dimension. Grain = product_id. Category is exposed in both the
-- original Portuguese and the English translation.
with products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    product_category_name          as product_category_pt,
    product_category_name_english  as product_category,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_photos_qty
from products
