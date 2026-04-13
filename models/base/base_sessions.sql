SELECT 
    "_fivetran_id" AS fivetran_id,
    "OS" AS operating_system,
    "IP" AS ip_address,
    "CLIENT_ID" AS client_id,
    "SESSION_ID" AS session_id,
    "SESSION_AT" AS session_at, 
    COALESCE("_fivetran_deleted", FALSE) AS is_deleted,
    "_fivetran_synced" AS fivetran_synced_at

FROM {{ source('web_schema', 'SESSIONS') }}