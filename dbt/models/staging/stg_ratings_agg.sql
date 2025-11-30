-- Staging model for ratings_agg.csv
-- Deduplicates by keeping the record with highest cnt_ratings 
-- (most recent cumulative value) for each date/listing_id combination

{{ config(materialized='view') }}

with source_data as (
    select
        date::date as rating_date,
        listing_id,
        cnt_ratings::integer as rating_count,
        avg_rating::numeric as avg_rating
    from {{ source('raw', 'ratings_agg') }}
    where cnt_ratings is not null
),

ranked as (
    select
        rating_date,
        listing_id,
        rating_count,
        avg_rating,
        row_number() over (
            partition by rating_date, listing_id 
            order by rating_count desc, avg_rating desc
        ) as rn
    from source_data
)

select
    rating_date,
    listing_id,
    rating_count,
    avg_rating
from ranked
where rn = 1
order by rating_date, listing_id
