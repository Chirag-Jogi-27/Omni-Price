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

ranked as (
  select
    *,
    lag(total_quantity) over (partition by asin order by valid_from) as prev_qty,
    lag(spec_hash) over (partition by asin order by valid_from) as prev_spec,
    lag(quantity_unit) over (partition by asin order by valid_from) as prev_unit
  from history
),

filtered as (
  select *
  from ranked
  where prev_qty is not null
    and total_quantity < prev_qty
    and quantity_unit = prev_unit
)

select
  asin || '_' || cast(valid_from as string) as spec_change_event_key,
  asin,
  brand,
  title as product_title,
  quantity_unit,
  prev_qty as old_quantity,
  total_quantity as new_quantity,
  round((prev_qty - total_quantity) / nullif(prev_qty, 0) * 100, 2) as shrink_pct,
  prev_spec as old_spec_hash,
  spec_hash as new_spec_hash,
  valid_from as change_date,
  valid_to
from filtered
