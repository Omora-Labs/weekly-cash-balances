-- Get latest asset value per month for each asset, grouped by currency

with asset_totals as (
    select * from {{ ref('monthly_asset_values') }} -- refers to dbt model
)

select
    month,
    currency_id,
    currency_name,
    count(distinct asset_id) as num_assets,
    sum(last_value) as total_value
from asset_totals
group by month, currency_name, currency_id
order by month, currency_name, currency_id
