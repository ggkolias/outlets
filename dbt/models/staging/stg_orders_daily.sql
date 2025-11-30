-- Staging model for orders_daily.csv
-- Deduplicates by keeping the latest timestamp for each date/listing_id combination

{{ config(materialized='view') }}

with source_data as (
    select
        date::date as order_date,
        listing_id,
        orders::integer as orders_count,
        timestamp::timestamp as updated_at
    from {{ source('raw', 'orders_daily') }}
),

ranked as (
    select
        order_date,
        listing_id,
        orders_count,
        updated_at,
        row_number() over (partition by order_date, listing_id order by updated_at desc) as rn
    from source_data
)

select
    order_date,
    listing_id,
    orders_count,
    updated_at
from ranked
where rn = 1
order by order_date, listing_id
