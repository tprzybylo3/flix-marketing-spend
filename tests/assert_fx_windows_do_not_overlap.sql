-- Error: two FX windows of the same currency that overlap, so a date would match two rates.
with rates as (
    select *, row_number() over (order by currency, valid_from_utc, valid_to_utc, rate) as rate_row_id
    from {{ ref('stg_finance__exchange_rates') }}
)

select a.currency, a.valid_from_utc, a.valid_to_utc, b.valid_from_utc as other_valid_from_utc, b.valid_to_utc as other_valid_to_utc
from rates a
join rates b
    on a.currency = b.currency
    and a.base_currency = b.base_currency
    and a.rate_row_id < b.rate_row_id
    and a.valid_from_utc < b.valid_to_utc
    and b.valid_from_utc < a.valid_to_utc
