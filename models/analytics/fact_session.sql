-- models/analytics/fact_session.sql
-- models/analytics/fact_session.sql
with sessions as (
    select * from {{ ref('int_sessions') }}
),
orders as (
    select 
        session_id,
        max(1) as has_placed_order
    from {{ ref('int_orders') }}
    group by 1
)
select
    s.session_id,
    s.has_viewed_shopping_page,
    s.has_viewed_item,
    s.has_added_to_cart,
    coalesce(o.has_placed_order, 0) as has_placed_order
from sessions s
left join orders o on s.session_id = o.session_id