-- Staging model for listing.csv
-- Deduplicates by keeping the latest timestamp for each listing_id

{{ config(materialized='view') }}

with source_data as (
    select
        id as listing_id,
        outlet_id,
        platform_id,
        timestamp::timestamp as created_at
    from {{ source('raw', 'listing') }}
),

ranked as (
    select
        listing_id,
        outlet_id,
        platform_id,
        created_at,
        row_number() over (partition by listing_id order by created_at desc) as rn
    from source_data
)

select
    listing_id,
    outlet_id,
    platform_id,
    created_at
from ranked
where rn = 1
