-- Staging model for org.csv
-- Deduplicates by keeping the earliest timestamp for each org_id
-- In the given csv file, there are no duplicates by org_id, logic will be kept though

{{ config(materialized='view', schema='staging') }}

with source_data as (
    select
        id as org_id,
        name as org_name,
        timestamp::timestamp as created_at
    from {{ source('raw', 'org') }}
),

deduplicated as (
    select
        org_id,
        org_name,
        min(created_at) as created_at
    from source_data
    group by org_id, org_name
)

select
    org_id,
    org_name,
    created_at
from deduplicated
order by org_id
