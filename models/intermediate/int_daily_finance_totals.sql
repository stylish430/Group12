-- models/intermediate/int_daily_finance_totals.sql
with daily_revenue as (
    select 
        date_trunc('day', order_at) as date,
        sum(shipping_cost_usd) as daily_revenue -- 这里可以加上你的商品总额
    from {{ ref('int_orders') }}
    where is_refunded = false
    group by 1
),
daily_expenses as (
    select 
        date_trunc('day', expense_date) as date,
        sum(expense_amount) as daily_expense_amount
    from {{ ref('base_expenses') }}
    group by 1
)
select 
    r.date as revenue_date,
    r.daily_revenue,
    e.date as expense_date,
    e.daily_expense_amount
from daily_revenue r
full outer join daily_expenses e on r.date = e.date