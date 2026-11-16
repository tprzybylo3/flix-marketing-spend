-- Writes the mart to output/daily_spend_eur.csv.
{{ config(materialized='external', location='output/daily_spend_eur.csv', format='csv') }}

select spend_date, country, brand, channel, spend_eur, campaign_count, last_extracted_at
from {{ ref('fct_daily_marketing_spend') }}
order by spend_date, country, brand, channel
