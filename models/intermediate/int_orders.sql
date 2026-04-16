-- models/intermediate/int_orders.sql
with orders as (
    select *,
           row_number() over (partition by order_id order by order_at asc) as rn
    from {{ ref('base_orders') }}
),
unique_orders as (
    select * from orders where rn = 1
)
select 
    -- 核心修复：对所有字段明确指定来自表 'o'
    o.order_id,
    o.session_id,
    cast(o.phone as varchar) as phone, -- 顺便修复之前发现的类型问题
    o.client_name,
    o.state,
    o.shipping_cost_usd,
    o.order_at,
    o.tax_rate,
    o.payment_method,
    -- 来自返回表的字段
    coalesce(r.is_refunded, false) as is_refunded,
    r.returned_at
from unique_orders o
left join {{ ref('base_hr_returns') }} r 
    on cast(o.order_id as varchar) = cast(r.order_id as varchar)