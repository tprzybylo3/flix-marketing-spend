-- Converts each campaign-day to EUR with the rate valid at noon UTC of the spend date and adds country, brand and channel.
{% set channel_source = allowed_var('channel_source', ['channel_type', 'ad_platform']) %}
{% set fx_method = allowed_var('fx_method', ['daily', 'monthly_average']) %}
with costs as (
    select * from {{ ref('int_google_ads__costs_latest') }}
),
accounts as (
    select * from {{ ref('stg_google_ads__accounts') }}
),
domain_country as (
    select * from {{ ref('domain_country') }}
),
eur_rates as (
    select * from {{ ref('stg_finance__exchange_rates') }}
    where base_currency = 'EUR'
),
calendar as (
    select unnest(generate_series(
        (select min(cast(valid_from_utc as date)) from eur_rates),
        (select max(cast(valid_to_utc as date)) from eur_rates),
        interval 1 day
    ))::date as rate_date
),
daily_rates as (
    select calendar.rate_date, eur_rates.currency, eur_rates.rate
    from calendar
    join eur_rates
        on calendar.rate_date + interval 12 hour >= eur_rates.valid_from_utc
        and calendar.rate_date + interval 12 hour < eur_rates.valid_to_utc
),
applied_rates as (
    select
        rate_date,
        currency,
        {% if fx_method == 'monthly_average' %}
        avg(rate) over (partition by currency, date_trunc('month', rate_date)) as rate
        {% else %}
        rate
        {% endif %}
    from daily_rates
)

select
    costs.spend_date,
    costs.account_id,
    costs.campaign_id,
    costs.extracted_at,
    coalesce(domain_country.country, 'UNKNOWN') as country,
    accounts.brand,
    accounts.{{ 'ad_platform' if channel_source == 'ad_platform' else 'channel' }} as channel,
    accounts.currency,
    costs.cost_micros / 1e6 as cost_local,
    case when accounts.currency = 'EUR' then 1.0 else applied_rates.rate end as fx_rate,
    costs.cost_micros / 1e6
        / case when accounts.currency = 'EUR' then 1.0 else applied_rates.rate end as cost_eur
from costs
left join accounts
    on costs.account_id = accounts.account_id
left join domain_country
    on accounts.domain = domain_country.domain
left join applied_rates
    on accounts.currency = applied_rates.currency
    and costs.spend_date = applied_rates.rate_date
