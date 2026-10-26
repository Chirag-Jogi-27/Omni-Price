{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='spec_change_event_key',
    file_format='delta'
  )
}}

with history as (
  select * from {{ source('silver', 'amazon_product_history') }}
),

ranked_history as (
  select
    asin,
    brand,
    title as product_title,
    spec_hash,
    total_quantity,
    quantity_unit,
    -- GUARD: quantity_source bhi lag ke saath track karo
    quantity_source,
    valid_from,
    valid_to,
    lag(total_quantity) over (partition by asin order by valid_from) as prev_qty,
    lag(spec_hash)   over (partition by asin order by valid_from) as prev_spec,
    lag(quantity_unit) over (partition by asin order by valid_from) as prev_unit,
    lag(quantity_source) over (partition by asin order by valid_from) as prev_source
  from history
),

quantity_drops as (
  select *
  from ranked_history
  where prev_qty is not null
    and total_quantity < prev_qty
    and quantity_unit = prev_unit
    -- GUARD: quantity drop tabhi real hai jab SOURCE bhi same rahe.
    -- net_quantity -> title flip (e.g. B09VXFB268: 45.5g -> 38g) parse-change
    -- hai, pack-change nahi — usko alert mat banao. (545 mein se 539 false the!)
    and quantity_source = prev_source
    and (prev_qty - total_quantity) / cast(prev_qty as double) >= 0.02
),

prices as (
  select asin, snapshot_date, price
  from {{ source('silver', 'amazon_price_observation') }}
  where price is not null and price > 0
),

pre_prices as (
  select
    qd.asin,
    qd.valid_from,
    avg(p.price) as pre_change_avg_price
  from quantity_drops qd
  join prices p
    on p.asin = qd.asin
   and p.snapshot_date >= date_sub(qd.valid_from, 7)
   and p.snapshot_date < qd.valid_from
  group by qd.asin, qd.valid_from
),

post_prices as (
  select
    qd.asin,
    qd.valid_from,
    avg(p.price) as post_change_avg_price
  from quantity_drops qd
  join prices p
    on p.asin = qd.asin
   and p.snapshot_date >= qd.valid_from
   and p.snapshot_date <= date_add(qd.valid_from, 7)
  group by qd.asin, qd.valid_from
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
)

select
  qd.asin || '_' || cast(qd.valid_from as string) as spec_change_event_key,
  qd.asin,
  qd.brand,
  qd.product_title,
  coalesce(bc.search_category, 'Unknown') as search_category,
  qd.quantity_unit,
  qd.prev_qty as old_quantity,
  qd.total_quantity as new_quantity,
  -- GUARD: audit columns — dashboard/dq me dikhai de ki quantity kahan se aayi
  qd.prev_source as old_quantity_source,
  qd.quantity_source as new_quantity_source,
  round((qd.prev_qty - qd.total_quantity) / nullif(qd.prev_qty, 0) * 100, 2) as shrink_pct,
  round(pre.pre_change_avg_price, 2) as old_avg_price,
  round(post.post_change_avg_price, 2) as new_avg_price,
  round(pre.pre_change_avg_price / nullif(qd.prev_qty, 0), 4) as old_unit_price,
  round(post.post_change_avg_price / nullif(qd.total_quantity, 0), 4) as new_unit_price,
  round(
    (
      (post.post_change_avg_price / nullif(qd.total_quantity, 0)) -
      (pre.pre_change_avg_price / nullif(qd.prev_qty, 0))
    ) / nullif(pre.pre_change_avg_price / nullif(qd.prev_qty, 0), 0) * 100,
    2
  ) as unit_price_surge_pct,
  case
    when pre.pre_change_avg_price is null or post.post_change_avg_price is null then false
    when post.post_change_avg_price >= (pre.pre_change_avg_price * 0.95) then true
    else false
  end as is_confirmed_shrinkflation,
  qd.prev_spec as old_spec_hash,
  qd.spec_hash as new_spec_hash,
  qd.valid_from as change_date,
  qd.valid_to
from quantity_drops qd
left join pre_prices pre
  on pre.asin = qd.asin and pre.valid_from = qd.valid_from
left join post_prices post
  on post.asin = qd.asin and post.valid_from = qd.valid_from
left join best_category bc
  on bc.asin = qd.asin