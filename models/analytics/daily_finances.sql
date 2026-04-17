-- models/analytics/daily_finances.sql
-- models/intermediate/int_daily_finance_totals.sql

/*
Assumption:
We do not have actual order line items.
We approximate revenue using:
SUM(price_per_unit * add_to_cart_quantity)

This is a proxy for GMV (Gross Merchandise Value).
*/

with item_revenue as (

    select 
        date_trunc('day', item_view_at) as date,
        sum(price_per_unit * add_to_cart_quantity) as estimated_gmv
    from {{ ref('base_item_views') }}
    group by 1

),

order_shipping as (

    select 
        date_trunc('day', order_at) as date,
        sum(shipping_cost_usd) as shipping_revenue
    from {{ ref('int_orders') }}
    where is_refunded = false
    group by 1

),

daily_revenue as (

    select 
        coalesce(i.date, o.date) as date,
        coalesce(i.estimated_gmv, 0) + coalesce(o.shipping_revenue, 0) as total_revenue
    from item_revenue i
    full outer join order_shipping o
        on i.date = o.date

),

daily_expenses as (

    select 
        date_trunc('day', expense_date) as date,
        sum(expense_amount) as total_expense
    from {{ ref('base_expenses') }}
    group by 1

)

select 
    coalesce(r.date, e.date) as log_date,
    coalesce(r.total_revenue, 0) as total_revenue,
    coalesce(e.total_expense, 0) as total_cost,
    coalesce(r.total_revenue, 0) - coalesce(e.total_expense, 0) as net_profit
from daily_revenue r
full outer join daily_expenses e
    on r.date = e.date