-- models/analytics/daily_finances.sql
select 
    coalesce(revenue_date, expense_date) as log_date,
    coalesce(daily_revenue, 0) as total_revenue,
    coalesce(daily_expense_amount, 0) as total_marketing_costs,
    -- 计算利润：收入 - 成本
    (coalesce(daily_revenue, 0) - coalesce(daily_expense_amount, 0)) as net_profit
from {{ ref('int_daily_finance_totals') }}