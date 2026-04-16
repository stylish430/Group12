-- models/analytics/fact_session.sql
select 
    s.session_id,
    s.client_id,
    s.session_at,
    s.operating_system,
    -- 漏斗标志位 (从中间层聚合表 int_session_events_agg 获取)
    f.has_viewed_shopping_page,
    f.has_viewed_item,
    f.has_added_to_cart,
    -- 下单标志位 (如果该 session 在订单表里存在，则为 1)
    case when o.order_id is not null then 1 else 0 end as has_placed_order
from {{ ref('int_sessions') }} s
left join {{ ref('int_session_events_agg') }} f on s.session_id = f.session_id
left join {{ ref('int_orders') }} o on s.session_id = o.session_id