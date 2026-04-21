-- models/analytics/dim_client.sql
with sessions as (
    select * from {{ ref('int_sessions') }}
),

orders as (
    select * from {{ ref('int_orders') }}
),

client_base as (
    select 
        client_id,
        min(session_at) as first_seen_at,
        count(distinct session_id) as total_sessions
    from sessions
    group by 1
),

latest_address as (
    select *
    from (
        select
            s.client_id,
            o.shipping_address,
            row_number() over (
                partition by s.client_id 
                order by o.order_at desc
            ) as rn
        from orders o
        join sessions s on o.session_id = s.session_id
    )
    where rn = 1
),

order_summary as (
    select 
        s.client_id,
        count(distinct o.order_id) as total_orders,
        sum(o.shipping_cost_usd) as lifetime_value
    from orders o
    join sessions s on o.session_id = s.session_id
    where o.is_refunded = false
    group by 1
)

select 
    b.client_id,
    b.first_seen_at,
    b.total_sessions,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.lifetime_value, 0) as lifetime_value,
    la.shipping_address as latest_shipping_address,
    case 
        when o.total_orders > 0 then 'Purchaser' 
        else 'Visitor' 
    end as client_category
from client_base b
left join order_summary o using (client_id)
left join latest_address la using (client_id)