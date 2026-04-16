-- models/intermediate/int_orders.sql
with orders as (
    select *,
           row_number() over (partition by order_id order by order_at asc) as rn
    from {{ ref('base_orders') }}
),
unique_orders as (
    select * from orders where rn = 1
),
-- 额外对退货表去重，防止 JOIN 导致数据膨胀
unique_returns as (
    select *,
           row_number() over (partition by order_id order by returned_at desc) as rn
    from {{ ref('base_hr_returns') }}
)
select 
    o.order_id,
    o.session_id,
    cast(o.phone as varchar) as phone,
    o.client_name,
    o.state,
    o.shipping_cost_usd,
    o.order_at,
    o.tax_rate,
    o.payment_method,
    coalesce(r.is_refunded, false) as is_refunded,
    r.returned_at
from unique_orders o
left join unique_returns r 
    on cast(o.order_id as varchar) = cast(r.order_id as varchar)
    and r.rn = 1 -- 确保一个订单只关联一条退货记录