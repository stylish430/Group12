-- models/analytics/fact_order.sql
select 
    order_id,
    session_id,
    client_name,
    phone as client_phone,
    state as shipping_state,
    payment_method,
    shipping_cost_usd as revenue, -- 收入指标
    tax_rate,
    order_at,
    is_refunded
from {{ ref('int_orders') }}