-- Daily spend in EUR per date x country x brand x channel; incremental, rebuilds the last lookback_days dates on each run.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='spend_date',
        on_schema_change='append_new_columns'
    )
}}
{% set run_date = var('run_date', none) %}

select
    spend_date,
    country,
    brand,
    channel,
    cast(round(sum(cost_eur), 2) as decimal(18, 2)) as spend_eur,
    count(distinct account_id || '|' || campaign_id) as campaign_count,
    max(extracted_at) as last_extracted_at
from {{ ref('int_google_ads__costs_eur') }}
where true
{% if is_incremental() %}
  and spend_date > least(
        {% if run_date %}date '{{ run_date }}'{% else %}(select max(spend_date) from {{ this }}){% endif %},
        (select max(spend_date) from {{ this }}) + 1
      ) - interval {{ var('lookback_days') }} day
{% endif %}
{% if run_date %}
  and spend_date <= date '{{ run_date }}'
{% endif %}
group by spend_date, country, brand, channel
