-- Staging model for rank.csv
-- Deduplicates by keeping the latest timestamp for each date/listing_id combination
-- Calculates average rank per day

{{ config(materialized='view') }}

with source_data as (
    select
        listing_id,
        date::date as rank_date,
        timestamp::timestamp as updated_at,
        is_online::boolean as is_online,
        case 
            when rank is null or trim(rank) = '' then null 
            else rank::numeric 
        end as rank_value
    from {{ source('raw', 'rank') }}
    where rank is not null and trim(rank) != ''
),

daily_avg_rank as (
    select
        listing_id,
        rank_date,
        avg(rank_value) as avg_rank,
        max(updated_at) as last_updated
    from source_data
    group by listing_id, rank_date
)

select
    listing_id,
    rank_date,
    avg_rank,
    last_updated
from daily_avg_rank
order by rank_date, listing_id
