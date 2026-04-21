-- models/analytics/dim_item.sql
select
    item_name,
    array_agg(distinct price_per_unit) as price_list
from {{ ref('base_item_views') }}
group by 1