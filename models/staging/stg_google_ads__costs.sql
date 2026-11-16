-- Raw Google Ads costs: renamed and typed, one row per raw row (duplicates and older versions are kept here).
-- trim() keeps try_cast lenient: without it DuckDB 2.0 pushes the cast into the CSV reader, which then fails on a bad value
select
    try_cast(trim(segments_date) as date) as spend_date,
    customer_id as account_id,
    campaign_id,
    campaign_name,
    try_cast(trim(last_extracted_at) as timestamp) as extracted_at,
    try_cast(trim(metrics_cost_micros) as bigint) as cost_micros
from {{ source('google_ads', 'de_case_study_anon_raw_costs') }}
