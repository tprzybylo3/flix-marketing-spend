-- Error: an FX rate that is not positive or a validity window that ends before it starts.
select *
from {{ ref('stg_finance__exchange_rates') }}
where rate <= 0
   or valid_to_utc <= valid_from_utc
