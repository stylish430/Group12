-- models/intermediate/int_orders.sql
with orders as (
    select *,
           -- 使用 ROW_NUMBER 仅保留每个 order_id 的第一个实例 [cite: 78, 79]
           row_number() over (partition by order_id order by order_at asc) as rn
    from {{ ref('base_orders') }}
),
unique_orders as (
    select * from orders where rn = 1
)
select 
    o.*,
    -- 关联退货表，判断是否已退款 [cite: 51]
    coalesce(r.is_refunded, false) as is_refunded,
    r.returned_at
from unique_orders o
left join {{ ref('base_hr_returns') }} r on o.order_id = r.order_id