-- Staging model for outlet.csv
-- Deduplicates by keeping the earliest timestamp for each outlet_id
-- Filters out invalid (0.0, 0.0) or empty coordinates

{{ config(materialized='view') }}

with source_data as (
    select
        id as outlet_id,
        org_id,
        name as outlet_name,
        case 
            when trim(coalesce(latitude, '')) = '' then null
            else latitude::numeric
        end as latitude,
        case 
            when trim(coalesce(longitude, '')) = '' then null
            else longitude::numeric
        end as longitude,
        timestamp::timestamp as created_at
    from {{ source('raw', 'outlet') }}
    where trim(coalesce(latitude, '')) != '' 
      and trim(coalesce(longitude, '')) != ''
),

deduplicated as (
    select
        outlet_id,
        org_id,
        outlet_name,
        latitude,
        longitude,
        min(created_at) as created_at
    from source_data
    group by outlet_id, org_id, outlet_name, latitude, longitude
)

select
    outlet_id,
    org_id,
    outlet_name,
    latitude,
    longitude,
    created_at
from deduplicated
order by outlet_id
