select
    cast(employee_id as varchar) as employee_id,
    try_to_date(quit_date) as quit_date,
    _file as source_file,
    _line as source_line,
    _modified::timestamp_ntz as modified_at,
    _fivetran_synced::timestamp_ntz as fivetran_synced_at
from {{ source('google_drive_schema', 'HR_QUITS') }}

