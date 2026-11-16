-- Warning: mart rows that differ from a fresh aggregation of the intermediate model (last drift_check_days, or all dates).
{{ config(severity='warn') }}

{% set run_date = var('run_date', none) %}
{% set check_days = var('drift_check_days', none) %}
{% set end_date %}{% if run_date %}date '{{ run_date }}'{% else %}(select max(spend_date) from {{ ref('fct_daily_marketing_spend') }}){% endif %}{% endset %}

with expected as (
    select spend_date, country, brand, channel, sum(cost_eur) as expected_eur
    from {{ ref('int_google_ads__costs_eur') }}
    where spend_date <= {{ end_date }}
    {% if check_days %}  and spend_date > {{ end_date }} - {{ check_days }}{% endif %}
    group by spend_date, country, brand, channel
),

actual as (
    select spend_date, country, brand, channel, spend_eur as actual_eur
    from {{ ref('fct_daily_marketing_spend') }}
    where spend_date <= {{ end_date }}
    {% if check_days %}  and spend_date > {{ end_date }} - {{ check_days }}{% endif %}
)

select *
from expected
full outer join actual using (spend_date, country, brand, channel)
where actual_eur is null
   or expected_eur is null
   or abs(expected_eur - actual_eur) > 0.01
