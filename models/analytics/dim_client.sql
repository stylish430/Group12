-- models/analytics/dim_client.sql
with client_base as (
    select 
        client_id,
        min(session_at) as first_seen_at,
        count(distinct session_id) as total_sessions
    from {{ ref('int_sessions') }}
    group by 1
),
order_summary as (
    select 
        phone as client_phone,
        client_name,
        count(distinct order_id) as total_orders,
        sum(shipping_cost_usd) as ltv
    from {{ ref('int_orders') }}
    where is_refunded = false
    group by 1, 2
)
select 
    b.client_id,
    o.client_name,
    b.first_seen_at,
    b.total_sessions,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.ltv, 0) as lifetime_value,
    case when o.total_orders > 0 then 'Purchaser' else 'Visitor' end as client_category
from client_base b
left join order_summary o on b.client_id = o.client_phone