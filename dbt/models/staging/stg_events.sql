{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

-- Events model: User behavior tracking and analytics  
-- Source: bronze_events_parquet (JSON string data)
-- Grain: One row per event

with source_data as (
    select
        -- Extract main event fields from JSON string
        json_extract_string(json, '$.event_id') as event_id,
        json_extract_string(json, '$.event_ts') as event_ts,
        json_extract_string(json, '$.event_type') as event_type,
        json_extract_string(json, '$.user_id')::integer as user_id,
        json_extract_string(json, '$.session_id') as session_id,
        json_extract_string(json, '$.payload') as payload_json,
        
        -- Partition columns from Hive-style partitioning
        event_dt,
        
        -- Metadata columns
        md5(json) as src_row_hash,
        ingestion_ts
    from bronze_events_parquet
    where json_extract_string(json, '$.event_id') is not null
),

final as (
    select
        -- Primary key
        event_id,
        
        -- Event details
        {{ convert_adelaide_to_utc('event_ts::timestamp') }} as event_ts,
        event_type,
        user_id,
        session_id,
        
        -- Payload parsing - extract all possible fields
        json_extract_string(payload_json, '$.order_id')::integer as purchase_order_id,
        json_extract_string(payload_json, '$.amount')::decimal(10,2) as purchase_amount,
        json_extract_string(payload_json, '$.product_id')::integer as cart_product_id,
        json_extract_string(payload_json, '$.qty')::integer as cart_qty,
        json_extract_string(payload_json, '$.query') as search_query,
        json_extract_string(payload_json, '$.page') as page_viewed,
        

        
        -- Metadata
        src_row_hash,
        ingestion_ts
        
    from source_data
    {% if is_incremental() %}
    where event_ts::timestamp > (select max(event_ts) from {{ this }})
    {% endif %}
)

select * from final
