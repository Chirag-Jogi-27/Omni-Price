-- Singular test: Selling price must be strictly positive in price_daily_enriched.
-- Free or negative prices violate FMCG retail pricing invariants.
select
    asin_snapshot_date,
    asin,
    price
from {{ ref('price_daily_enriched') }}
where price <= 0
