-- Warning: raw rows whose extraction time is before the spend date.
{{ config(severity='warn') }}

select *
from {{ ref('stg_google_ads__costs') }}
where extracted_at < spend_date
