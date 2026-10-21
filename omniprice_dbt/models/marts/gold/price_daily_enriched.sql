{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='asin_snapshot_date',
    file_format='delta',
    on_schema_change='append_new_columns'
  )
}}

with price as (
  select
    asin,
    snapshot_date,
    case when coalesce(price, 0) = 0 then null else price end as price,
    mrp,
    discount_pct,
    is_mrp_imputed,
    is_price_gt_mrp,
    case when coalesce(price, 0) = 0 then true else is_price_missing end as is_price_missing,
    case when coalesce(price, 0) = 0 then 'zero_price_captured' else price_missing_reason end as price_missing_reason,
    availability_status,
    observation_source,
    rating,
    review_count,
    brand as price_brand
  from {{ source('silver', 'amazon_price_observation') }}
  {% if is_incremental() %}
  where snapshot_date >= (select date_sub(coalesce(max(snapshot_date), '1900-01-01'), 3) from {{ this }})
  {% endif %}
),

history as (
  select * from {{ source('silver', 'amazon_product_history') }}
),

best_category as (
  select asin, search_category
  from (
    select asin, search_category,
           row_number() over (
             partition by asin
             order by cnt desc, max_date desc
           ) as rn
    from (
      select asin, search_category,
             count(*) as cnt,
             max(snapshot_date) as max_date
      from {{ source('silver', 'amazon_search_observation') }}
      where search_category is not null
      group by asin, search_category
    ) freq
  ) ranked
  where rn = 1
),

search_title as (
  select asin, title
  from (
    select asin, title,
           row_number() over (partition by asin order by snapshot_date desc) as rn
    from {{ source('silver', 'amazon_search_observation') }}
    where title is not null
  ) t
  where rn = 1
)

select
  p.asin || '_' || cast(p.snapshot_date as string) as asin_snapshot_date,
  p.asin,
  p.snapshot_date,
  p.price,
  p.mrp,
  p.discount_pct,
  p.is_mrp_imputed,
  p.is_price_gt_mrp,
  p.is_price_missing,
  p.price_missing_reason,
  p.availability_status,
  p.observation_source,
  p.rating,
  p.review_count,
  coalesce(h.title, st.title) as product_title,
  coalesce(h.brand, p.price_brand) as brand,
  coalesce(bc.search_category, 'PDP Only') as search_category,
  h.total_quantity,
  h.quantity_unit,
  h.quantity_source,
  h.pack_count,
  h.is_multipack,
  h.unit_quantity,
  h.spec_hash,
  h.manufacturer,
  h.country_of_origin,
  case
    when h.quantity_unit = 'count' then round(p.price / nullif(h.total_quantity, 0), 2)
    else round((p.price / nullif(h.total_quantity, 0)) * 100, 2)
  end as price_per_100_on_price_date,
  h.valid_from,
  h.valid_to,
  coalesce(h.is_current, true) as is_current
from price p
left join history h
  on h.asin = p.asin
 and p.snapshot_date >= h.valid_from
 and (p.snapshot_date <= h.valid_to or h.valid_to = '9999-12-31')
left join best_category bc
  on bc.asin = p.asin
left join search_title st
  on st.asin = p.asin