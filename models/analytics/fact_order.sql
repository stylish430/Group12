-- models/analytics/fact_order.sql
select 
    o.order_id,
    o.session_id,
    s.client_id,
    o.client_name,
    o.phone as client_phone,
    o.state as shipping_state,
    o.shipping_address,
    o.payment_method,
    o.shipping_cost_usd,
    o.tax_rate,
    o.order_at,
    o.is_refunded,
    o.returned_at
from {{ ref('int_orders') }} o
left join {{ ref('int_sessions') }} s
    on o.session_id = s.session_id