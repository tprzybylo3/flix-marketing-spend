-- Warning: dates whose source has rows extracted after the mart's last_extracted_at, i.e. a late correction outside the window.
{{ config(severity='warn') }}

select
    costs.spend_date,
    count(*) as late_rows,
    max(costs.extracted_at) as newest_extraction,
    mart.last_extracted_at
from {{ ref('stg_google_ads__costs') }} as costs
join (
    select spend_date, max(last_extracted_at) as last_extracted_at
    from {{ ref('fct_daily_marketing_spend') }}
    group by spend_date
) as mart
    on costs.spend_date = mart.spend_date
where costs.extracted_at > mart.last_extracted_at
group by costs.spend_date, mart.last_extracted_at
