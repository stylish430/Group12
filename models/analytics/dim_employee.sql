select 
    employee_id,
    employee_name,
    job_title,
    annual_salary,
    hire_date,
    quit_date,
    is_active
from {{ ref('int_hr_employees') }}