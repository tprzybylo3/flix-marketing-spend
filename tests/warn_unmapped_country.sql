-- Warning: accounts whose domain is not in seeds/domain_country.csv (country = UNKNOWN).
{{ config(severity='warn') }}

select distinct account_id
from {{ ref('int_google_ads__costs_eur') }}
where country = 'UNKNOWN'
