-- models/intermediate/int_orders.sql
with orders as (
    select *,
           row_number() over (partition by order_id order by order_at asc) as rn
    from {{ ref('base_orders') }}
),

unique_orders as (
    select * from orders where rn = 1
),

returns as (
    select *,
           row_number() over (partition by order_id order by returned_at desc) as rn
    from {{ ref('base_hr_returns') }}
),

latest_returns as (
    select * from returns where rn = 1
)

select 
    o.order_id,
    o.session_id,
    o.client_name,
    o.phone,
    o.state,
    o.shipping_address,
    o.shipping_cost_usd,
    o.order_at,
    o.tax_rate,
    o.payment_method,
    coalesce(r.is_refunded, false) as is_refunded,
    r.returned_at
from unique_orders o
left join latest_returns r
    on o.order_id = r.order_id