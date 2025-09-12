{{ config(
    materialized='incremental',
    unique_key='shipment_id',
    contract={'enforced': true},
    on_schema_change='fail'
) }}

-- Incremental staging model for shipments from Bronze parquet files



with src as (
  select * from bronze_shipments_parquet
  
  {% if is_incremental() %}
    -- Incremental logic: only process new/updated records
    where ingestion_ts > (select max(ingestion_ts) from {{ this }})
  {% endif %}
),

-- 1) Clean and standardize data types according to schema
normalized as (
  select
    -- Schema columns - exact mapping
    cast(shipment_id as bigint)                                    as shipment_id,
    cast(order_id as bigint)                                       as order_id,
    cast(upper(trim(carrier)) as varchar)                          as carrier,
    {{ convert_adelaide_to_utc('shipped_at') }}                    as shipped_at,
    case 
      when delivered_at is not null then {{ convert_adelaide_to_utc('delivered_at') }}
      else null 
    end                                                            as delivered_at,
    cast(ship_cost as decimal(12,2))                               as ship_cost,
    
    -- Bronze metadata columns  
    {{ current_utc_timestamp() }}                                  as ingestion_ts,
    cast('shipments.parquet' as varchar)                          as src_filename,
    cast(src_row_hash as varchar)                                 as src_row_hash
  from src
),

-- 2) Enforce key constraints and data quality
filtered as (
  select *
  from normalized
  where shipment_id is not null
    and order_id is not null
    and carrier is not null
    and shipped_at is not null
    and ship_cost >= 0
    -- delivered_at can be null for in-transit shipments
),

-- 3) Keep only shipments with valid order_id
with_valid_orders as (
  select f.*
  from filtered f
  where exists (
    select 1
    from {{ ref('stg_orders_header') }} oh
    where oh.order_id = f.order_id
  )
),

-- 4) Deduplicate on shipment_id
deduplicated as (
  select * from (
    select *,
      row_number() over (
        partition by shipment_id 
        order by ingestion_ts desc
      ) as rn
    from with_valid_orders
  )
  where rn = 1
)

select
  -- Schema columns only - exact mapping from shipments_schema
  shipment_id,
  order_id,
  carrier,
  shipped_at,
  delivered_at,
  ship_cost,
  
  -- Metadata columns (exceptions allowed)
  ingestion_ts,
  src_filename,
  src_row_hash
  
from deduplicated
