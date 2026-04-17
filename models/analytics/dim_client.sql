-- models/analytics/dim_client.sql
with client_base as (

    select 
        client_id,
        min(session_at) as first_seen_at,
        count(distinct session_id) as total_sessions
    from {{ ref('int_sessions') }}
    group by 1

),

orders_enriched as (

    select 
        s.client_id,
        o.order_id,
        o.shipping_cost_usd
    from {{ ref('int_orders') }} o
    left join {{ ref('int_sessions') }} s
        on o.session_id = s.session_id
    where o.is_refunded = false

),

order_summary as (

    select 
        client_id,
        count(distinct order_id) as total_orders,
        sum(shipping_cost_usd) as ltv
    from orders_enriched
    group by 1

)

select 
    b.client_id,
    b.first_seen_at,
    b.total_sessions,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.ltv, 0) as lifetime_value,
    case when o.total_orders > 0 then 'Purchaser' else 'Visitor' end as client_category
from client_base b
left join order_summary o 
    on b.client_id = o.client_id