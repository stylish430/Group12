-- models/intermediate/int_hr_employees.sql
with joins as (
    select * from {{ ref('base_hr_joins') }}
),
quits as (
    select * from {{ ref('base_hr_quits') }}
)

select
    j.employee_id,
    j.employee_name,
    j.job_title,
    j.annual_salary,
    j.hire_date,
    q.quit_date,
    case when q.quit_date is null then true else false end as is_active
from joins j
left join quits q 
    on j.employee_id = q.employee_id