-- Staging model for orders.csv
-- Deduplicates by order_id (keeping latest status if duplicates exist)
-- In the csv file given, there are not duplicates by order_id, logic will be kept though

{{ config(materialized='view') }}

with source_data as (
    select
        listing_id,
        order_id,
        placed_at::timestamp as placed_at,
        status
    from {{ source('raw', 'orders') }}
),

deduplicated as (
    select
        listing_id,
        order_id,
        placed_at,
        status,
        row_number() over (
            partition by order_id 
            order by placed_at desc
        ) as rn
    from source_data
)

select
    listing_id,
    order_id,
    placed_at,
    status
from deduplicated
where rn = 1
order by placed_at
