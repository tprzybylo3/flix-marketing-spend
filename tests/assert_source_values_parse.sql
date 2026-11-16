-- Error: a raw value that is not a valid date, timestamp or number; failing rows are stored with table, column and value.
{{ config(severity='error', store_failures=true) }}

with costs as (
    select * from {{ source('google_ads', 'de_case_study_anon_raw_costs') }}
),

rates as (
    select * from {{ source('finance', 'de_case_study_exchange_rates') }}
),

checks as (
    select 'raw_costs' as source_table, 'SEGMENTS_DATE' as column_name, segments_date as raw_value,
           customer_id || ' / ' || campaign_id as row_key, try_cast(segments_date as date) is null as is_bad
    from costs where segments_date is not null
    union all
    select 'raw_costs', 'LAST_EXTRACTED_AT', last_extracted_at,
           customer_id || ' / ' || campaign_id || ' / ' || segments_date, try_cast(last_extracted_at as timestamp) is null
    from costs where last_extracted_at is not null
    union all
    select 'raw_costs', 'METRICS_COST_MICROS', metrics_cost_micros,
           customer_id || ' / ' || campaign_id || ' / ' || segments_date, try_cast(metrics_cost_micros as bigint) is null
    from costs where metrics_cost_micros is not null
    union all
    select 'exchange_rates', 'RATE', rate, currency || ' / ' || valid_from, try_cast(rate as double) is null
    from rates where rate is not null
    union all
    select 'exchange_rates', 'VALID_FROM', valid_from, currency, try_strptime(valid_from, '%Y-%m-%d %H:%M:%S.%g %z') is null
    from rates where valid_from is not null
    union all
    select 'exchange_rates', 'VALID_TO', valid_to, currency || ' / ' || valid_from, try_strptime(valid_to, '%Y-%m-%d %H:%M:%S.%g %z') is null
    from rates where valid_to is not null
)

select source_table, column_name, raw_value, row_key
from checks
where is_bad
