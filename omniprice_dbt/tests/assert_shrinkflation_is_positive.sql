-- Singular test: Shrinkflation alert must only contain true quantity reductions (shrink_pct > 0).
-- If this query returns any rows, the test fails.
select
    spec_change_event_key,
    asin,
    shrink_pct
from {{ ref('shrinkflation_alert') }}
where shrink_pct <= 0
