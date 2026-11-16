-- Error: a negative cost.
select *
from {{ ref('stg_google_ads__costs') }}
where cost_micros < 0
