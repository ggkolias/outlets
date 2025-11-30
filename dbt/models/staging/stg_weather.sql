-- Staging model for weather data from CSV file(s)
-- Combines all weather CSV files and deduplicates

{{ config(materialized='view') }}


select
    outlet_id,
    outlet_name,
    latitude,
    longitude,
    datetime::timestamp as weather_time,
    wind_speed_10m::numeric as wind_speed_10m,
    temperature_2m::numeric as temperature_2m,
    relative_humidity_2m::numeric as relative_humidity_2m
from {{ source('raw', 'weather') }}
where datetime is not null
  and wind_speed_10m is not null
  and temperature_2m is not null
  and relative_humidity_2m is not null
