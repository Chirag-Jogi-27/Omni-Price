with source as (
        select * from {{ source('silver', 'amazon_search_observation') }}
  ),
  renamed as (
      select
          {{ adapter.quote("asin") }},
        {{ adapter.quote("search_category") }},
        {{ adapter.quote("snapshot_date") }},
        {{ adapter.quote("price") }},
        {{ adapter.quote("mrp") }},
        {{ adapter.quote("is_mrp_imputed") }},
        {{ adapter.quote("discount_amount") }},
        {{ adapter.quote("discount_pct") }},
        {{ adapter.quote("rating") }},
        {{ adapter.quote("review_count") }},
        {{ adapter.quote("title") }},
        {{ adapter.quote("raw_badge") }},
        {{ adapter.quote("raw_coupon_badge") }},
        {{ adapter.quote("sales_volume_tag") }},
        {{ adapter.quote("is_sponsored") }},
        {{ adapter.quote("page") }},
        {{ adapter.quote("position_on_page") }},
        {{ adapter.quote("scraped_at") }},
        {{ adapter.quote("micro_category") }},
        {{ adapter.quote("raw_unit_price") }},
        {{ adapter.quote("shelf_hash") }}

      from source
  )
  select * from renamed
    