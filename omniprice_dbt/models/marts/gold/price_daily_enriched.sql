{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='asin_snapshot_date',
    file_format='delta'
  )
}}

with price as (
  select distinct
    asin,
    snapshot_date,
    price,
    mrp,
    discount_pct,
    is_mrp_imputed,
    is_price_gt_mrp,
    observation_source,
    rating,
    review_count
  from {{ source('silver', 'amazon_price_observation') }}
  {% if is_incremental() %}
  where snapshot_date > (select coalesce(max(snapshot_date), '1900-01-01') from {{ this }})
  {% endif %}
),

history as (
  select * from {{ source('silver', 'amazon_product_history') }}
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
  p.observation_source,
  p.rating,
  p.review_count,
  h.title as product_title,
  h.brand,
  h.total_quantity,
  h.quantity_unit,
  h.quantity_source,
  h.pack_count,
  h.is_multipack,
  h.unit_quantity,
  h.spec_hash,
  h.manufacturer,
  h.country_of_origin,
  -- Clean unit economics: price per piece for count items, price per 100g/100ml for weight/volume
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
 and p.snapshot_date between h.valid_from and h.valid_to
