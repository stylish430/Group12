select
    _FILE as source_file,
    _LINE as source_line,
    DATE as expense_date,
    EXPENSE_TYPE as expense_type,
    try_cast(
        replace(
            replace(EXPENSE_AMOUNT, '$', ''),
            ',',
            ''
        ) as numeric(18,2)
    ) as expense_amount,
    _MODIFIED as source_modified_ts,
    _FIVETRAN_SYNCED as fivetran_synced_ts
from {{ source('google_drive_schema', 'EXPENSES') }}