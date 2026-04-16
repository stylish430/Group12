-- models/intermediate/int_hr_employees.sql
with joins as (
    select * from {{ ref('base_hr_joins') }}
),
quits as (
    select * from {{ ref('base_hr_quits') }}
),
returns as (
    -- 假设 base_hr_returns 中包含 order 退回，
    -- 如果有员工回归表请替换为对应的 base 表名
    select * from {{ ref('base_hr_returns') }} 
)
select 
    j.employee_id,
    j.employee_name,
    j.job_title,
    j.annual_salary,
    j.hire_date,
    q.quit_date,
    -- 判断员工是否在职
    case when q.quit_date is null then true else false end as is_active
from joins j
left join quits q on j.employee_id = q.employee_id