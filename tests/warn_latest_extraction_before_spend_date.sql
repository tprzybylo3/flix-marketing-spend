-- Warning: the chosen latest version was extracted before its spend date (normal in a daily feed until the main extraction).
{{ config(severity='warn') }}

select *
from {{ ref('int_google_ads__costs_latest') }}
where extracted_at < spend_date
