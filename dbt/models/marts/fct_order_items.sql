
-- Central fact of the star schema.
-- Grain: one row per item line within an order (order_id + order_item_id).
-- Foreign keys point at dim_customers, dim_products, dim_sellers and dim_date;
-- measures are the item price, freight, and derived delivery timings.
--
-- Loaded INCREMENTALLY on order_purchase_timestamp: a full/`--full-refresh` run
-- builds every row, while a normal (daily) run only processes orders at/after
-- the current high-water mark and delete+inserts them by order_item_key — so
-- re-running the same day is idempotent and cheap. (The bronze layer is a daily
-- full refresh because the Olist source is a static historical dump.)
{{ config(
    materialized='incremental',
    unique_key='order_item_key',
    incremental_strategy='delete+insert'
) }}

with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
)

select
    -- surrogate primary key for the grain
    i.order_id || '-' || i.order_item_id          as order_item_key,

    -- degenerate / natural keys
    i.order_id,
    i.order_item_id,

    -- foreign keys -> dimensions
    o.customer_id,
    i.product_id,
    i.seller_id,
    cast(to_char(o.order_purchase_timestamp, 'YYYYMMDD') as int)
                                                  as order_purchase_date_key,

    -- order context
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- measures
    i.price,
    i.freight_value,
    (i.price + i.freight_value)                   as item_total_value,

    -- delivery performance (days); positive = delivered before estimate
    extract(epoch from (o.order_delivered_customer_date - o.order_purchase_timestamp))
        / 86400.0                                 as delivery_days,
    extract(epoch from (o.order_estimated_delivery_date - o.order_delivered_customer_date))
        / 86400.0                                 as delivery_vs_estimate_days
from items i
inner join orders o
    on i.order_id = o.order_id
{% if is_incremental() %}
    -- only (re)process orders at/after the latest purchase already loaded
    where o.order_purchase_timestamp >= (select max(order_purchase_timestamp) from {{ this }})
{% endif %}
