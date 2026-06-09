-- Customer dimension. Grain = customer_id (order-scoped key that the fact
-- joins on). customer_unique_id is carried so analysts can still roll up to
-- the real person.
with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from customers
