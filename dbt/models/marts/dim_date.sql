-- Date dimension spanning the full range of order purchase timestamps.
-- date_key is a YYYYMMDD integer that the fact references.
with bounds as (
    select
        min(order_purchase_timestamp)::date as min_date,
        max(order_purchase_timestamp)::date as max_date
    from {{ ref('stg_orders') }}
),

spine as (
    select generate_series(min_date, max_date, interval '1 day')::date as date_day
    from bounds
)

select
    cast(to_char(date_day, 'YYYYMMDD') as int) as date_key,
    date_day                                   as date,
    extract(year    from date_day)::int        as year,
    extract(quarter from date_day)::int        as quarter,
    extract(month   from date_day)::int        as month,
    to_char(date_day, 'Month')                 as month_name,
    extract(day     from date_day)::int        as day_of_month,
    extract(isodow  from date_day)::int        as day_of_week,
    to_char(date_day, 'Day')                   as day_name,
    extract(week    from date_day)::int        as week_of_year,
    (extract(isodow from date_day) in (6, 7))  as is_weekend
from spine
