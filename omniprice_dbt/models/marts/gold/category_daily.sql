{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='micro_category__search_category__snapshot_date',
    file_format='delta'
  )
}}

select
  coalesce(micro_category, 'unknown') || '_' ||
  coalesce(search_category, 'unknown') || '_' ||
  cast(snapshot_date as string) as micro_category__search_category__snapshot_date,
  snapshot_date,
  micro_category,
  search_category,
  count(distinct asin) as product_count,
  round(avg(price), 2) as avg_price,
  round(avg(discount_pct), 2) as avg_discount,
  round(avg(rating), 2) as avg_rating,
  round(
    sum(case when coalesce(cast(is_sponsored as boolean), false) then 1 else 0 end)
    / nullif(count(*), 0),
    4
  ) as sponsored_share
from {{ source('silver', 'amazon_search_observation') }}
{% if is_incremental() %}
where snapshot_date >= (select date_sub(coalesce(max(snapshot_date), '1900-01-01'), 3) from {{ this }})
{% endif %}
group by snapshot_date, micro_category, search_category