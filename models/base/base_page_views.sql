SELECT 
    "_fivetran_id" AS fivetran_id,
    "PAGE_NAME" AS page_name,
    "SESSION_ID" AS session_id,
    "VIEW_AT" AS view_at_ts,
    COALESCE(TRY_CAST("_fivetran_deleted" AS BOOLEAN), "_fivetran_deleted"::BOOLEAN, FALSE) AS is_deleted,
    "_fivetran_synced" AS fivetran_synced_at
    
FROM {{ source('web_schema', 'PAGE_VIEWS') }}