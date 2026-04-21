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
    s.*,
    coalesce(o.has_placed_order, 0) as has_placed_order
from sessions s
left join orders o using (session_id)