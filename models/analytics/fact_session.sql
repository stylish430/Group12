-- models/analytics/fact_session.sql
with session_base as (
    select * from {{ ref('int_sessions') }}
),
event_agg as (
    select * from {{ ref('int_session_events_agg') }}
),
-- 关键修复：确保一个 session 只对应一个下单标志，防止 session 下多单导致膨胀
order_flag as (
    select 
        session_id,
        max(1) as has_placed_order
    from {{ ref('int_orders') }}
    group by 1
)
select 
    s.session_id,
    s.client_id,
    s.session_at,
    s.operating_system,
    coalesce(f.has_viewed_shopping_page, 0) as has_viewed_shopping_page,
    coalesce(f.has_viewed_item, 0) as has_viewed_item,
    coalesce(f.has_added_to_cart, 0) as has_added_to_cart,
    coalesce(o.has_placed_order, 0) as has_placed_order
from session_base s
left join event_agg f on s.session_id = f.session_id
left join order_flag o on s.session_id = o.session_id