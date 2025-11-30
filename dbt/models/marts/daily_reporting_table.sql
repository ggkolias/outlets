-- Daily reporting table combining all data sources
-- Contains: listing, outlet, org, platform, date, orders, aggregated_orders, 
-- ratings (cumulative and delta), and rank (average)

{{ config(materialized='table') }}

with daily_orders as (
    select
        order_date as report_date,
        listing_id,
        sum(orders_count) as aggregated_orders
    from {{ ref('stg_orders_daily') }}
    group by order_date, listing_id
),

daily_order_transactions as (
    select
        date_trunc('day', placed_at)::date as report_date,
        listing_id,
        count(*) as order_count
    from {{ ref('stg_orders') }}
    group by date_trunc('day', placed_at)::date, listing_id
),

ratings_cumulative as (
    select
        rating_date as report_date,
        listing_id,
        rating_count,
        avg_rating,
        sum(rating_count) over (
            partition by listing_id 
            order by rating_date 
            rows between unbounded preceding and current row
        ) as cumulative_rating_count,
        avg(avg_rating) over (
            partition by listing_id 
            order by rating_date 
            rows between unbounded preceding and current row
        ) as cumulative_avg_rating
    from {{ ref('stg_ratings_agg') }}
),

ratings_delta as (
    select
        report_date,
        listing_id,
        rating_count,
        avg_rating,
        cumulative_rating_count,
        cumulative_avg_rating,
        rating_count - lag(rating_count, 1, 0) over (
            partition by listing_id 
            order by report_date
        ) as rating_count_delta,
        avg_rating - lag(avg_rating, 1, 0) over (
            partition by listing_id 
            order by report_date
        ) as avg_rating_delta
    from ratings_cumulative
),

daily_rank as (
    select
        rank_date as report_date,
        listing_id,
        avg_rank,
        last_updated
    from {{ ref('stg_rank') }}
),

-- Combine all dates from all fact tables
all_fact_dates as (
    select distinct listing_id, report_date
    from (
        select listing_id, report_date from daily_orders
        union
        select listing_id, report_date from daily_order_transactions
        union
        select listing_id, report_date from ratings_delta
        union
        select listing_id, report_date from daily_rank
    ) all_dates
)

select
    -- Date and identifiers
    afd.report_date,
    l.listing_id,
    l.outlet_id,
    o.outlet_name,
    o.latitude,
    o.longitude,
    l.platform_id,
    p.platform_name,
    p.platform_group,
    o.org_id,
    org.org_name,
    
    -- Orders
    coalesce(dot.order_count, 0) as orders,
    coalesce(daily_orders_agg.aggregated_orders, 0) as aggregated_orders,
    
    -- Ratings (cumulative and delta)
    coalesce(rd.rating_count, 0) as rating_count,
    rd.avg_rating,
    coalesce(rd.cumulative_rating_count, 0) as cumulative_rating_count,
    rd.cumulative_avg_rating,
    coalesce(rd.rating_count_delta, 0) as rating_count_delta,
    coalesce(rd.avg_rating_delta, 0) as avg_rating_delta,
    
    -- Rank (average per day)
    dr.avg_rank,
    dr.last_updated as rank_last_updated

from {{ ref('stg_listing') }} l
inner join {{ ref('stg_outlet') }} o on l.outlet_id = o.outlet_id
inner join {{ ref('stg_platform') }} p on l.platform_id = p.platform_id
inner join {{ ref('stg_org') }} org on o.org_id = org.org_id
inner join all_fact_dates afd on l.listing_id = afd.listing_id
left join daily_orders daily_orders_agg on afd.listing_id = daily_orders_agg.listing_id 
    and afd.report_date = daily_orders_agg.report_date
left join daily_order_transactions dot on afd.listing_id = dot.listing_id 
    and afd.report_date = dot.report_date
left join ratings_delta rd on afd.listing_id = rd.listing_id
    and afd.report_date = rd.report_date
left join daily_rank dr on afd.listing_id = dr.listing_id
    and afd.report_date = dr.report_date

order by afd.report_date desc, l.listing_id
