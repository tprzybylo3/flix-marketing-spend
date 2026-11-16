-- FX rates per currency with validity windows converted from CET to UTC.
-- trim() keeps try_cast lenient: without it DuckDB 2.0 pushes the cast into the CSV reader, which then fails on a bad value
select
    upper(currency) as currency,
    upper(base) as base_currency,
    timezone('UTC', try_strptime(valid_from, '%Y-%m-%d %H:%M:%S.%g %z')) as valid_from_utc,
    timezone('UTC', try_strptime(valid_to, '%Y-%m-%d %H:%M:%S.%g %z')) as valid_to_utc,
    try_cast(trim(rate) as double) as rate
from {{ source('finance', 'de_case_study_exchange_rates') }}
