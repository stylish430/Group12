-- models/analytics/dim_item.sql
select distinct
    item_name,
    avg(price_per_unit) as avg_price_usd
from {{ ref('base_item_views') }}
group by 1