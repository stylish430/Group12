select
    _FILE as source_file,
    _LINE as source_line,
    EMPLOYEE_ID as employee_id,
    try_to_date(replace(HIRE_DATE, 'day ', '')) as hire_date,
    NAME as employee_name,
    CITY as city,
    ADDRESS as address,
    TITLE as job_title,
    ANNUAL_SALARY as annual_salary,
    _MODIFIED as source_modified_ts,
    _FIVETRAN_SYNCED as fivetran_synced_ts
from {{ source('google_drive_schema', 'HR_JOINS') }}