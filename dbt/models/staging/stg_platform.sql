-- Staging model for platform.csv

{{ config(materialized='view') }}

select
    id as platform_id,
    "group" as platform_group,
    name as platform_name,
    country
from {{ source('raw', 'platform') }}
order by platform_id
