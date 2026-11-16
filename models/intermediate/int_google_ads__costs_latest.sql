-- Latest version of each campaign-day: newest extraction wins, a same-timestamp tie takes the higher cost (see vars).
{% set latest_grain = allowed_var('latest_grain', ['campaign', 'account_snapshot']) %}
{% set tie_break = allowed_var('tie_break', ['highest', 'lowest']) %}
with costs as (
    select * from {{ ref('stg_google_ads__costs') }}
),
scoped_costs as (
    select * from costs
    {% if latest_grain == 'account_snapshot' %}
    qualify extracted_at = max(extracted_at) over (partition by spend_date, account_id)
    {% endif %}
)

select
    spend_date,
    account_id,
    campaign_id,
    extracted_at,
    cost_micros
from scoped_costs
qualify row_number() over (
    partition by spend_date, account_id, campaign_id
    order by extracted_at desc, cost_micros {{ 'desc' if tie_break == 'highest' else 'asc' }}
) = 1
