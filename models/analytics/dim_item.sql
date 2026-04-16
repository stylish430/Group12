-- models/analytics/dim_item.sql
select 
    item_name,
    count(*) as total_views, -- 总浏览次数
    count(distinct session_id) as unique_session_views, -- 多少个会话看过它
    sum(add_to_cart_quantity) as total_added_to_cart, -- 总加购数量
    avg(price_per_unit) as avg_price_usd
from {{ ref('base_item_views') }}
group by 1