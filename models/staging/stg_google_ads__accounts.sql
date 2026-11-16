-- Account attributes (currency, brand, channel, domain), trimmed and with empty strings turned into NULL.
select
    account_id,
    upper(currency) as currency,
    nullif(trim(channel_type), '') as channel,
    nullif(trim(ad_platform), '') as ad_platform,
    nullif(trim(brand), '') as brand,
    nullif(trim(domain), '') as domain
from {{ source('google_ads', 'de_case_study_anon_account_info') }}
