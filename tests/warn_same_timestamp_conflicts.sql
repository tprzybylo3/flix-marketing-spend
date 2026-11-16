-- Warning: campaign-days with two different costs at the same extraction time (ties).
{{ config(severity='warn') }}

select spend_date, account_id, campaign_id, extracted_at, count(distinct cost_micros) as distinct_costs
from {{ ref('stg_google_ads__costs') }}
group by spend_date, account_id, campaign_id, extracted_at
having count(distinct cost_micros) > 1
