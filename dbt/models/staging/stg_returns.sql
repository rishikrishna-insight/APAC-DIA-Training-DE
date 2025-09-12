{{ config(materialized='table') }}

-- Base columns from returns_day1_schema:
-- return_id (int64), order_id (int64), product_id (int64), return_ts (timestamp), 
-- qty (int32), reason (string)
-- Additional columns will be handled dynamically

with src as (
  select * from bronze_returns_parquet
),

-- 1) Get all source columns for schema evolution support
source_with_metadata as (
  select 
    *,
    {{ current_utc_timestamp() }} as ingestion_ts,
    'returns.parquet' as src_filename,
    md5(concat_ws('|', 
      coalesce(cast(return_id as varchar), ''),
      coalesce(cast(order_id as varchar), ''),
      coalesce(cast(product_id as varchar), ''),
      coalesce(cast(return_ts as varchar), ''),
      coalesce(cast(qty as varchar), ''),
      coalesce(reason, '')
    )) as src_row_hash
  from src
),

-- 2) Transform base columns while preserving additional columns
normalized as (
  select
    -- Transform known base columns (returns_day1_schema)
    cast(return_id as bigint) as return_id,
    cast(order_id as bigint) as order_id,
    cast(product_id as bigint) as product_id,
    {{ convert_adelaide_to_utc('return_ts') }} as return_ts,
    cast(qty as integer) as qty,
    cast(upper(trim(reason)) as varchar) as reason,
    
    -- Metadata columns
    ingestion_ts,
    src_filename,
    src_row_hash,
    
    -- Handle schema evolution: return_reason_code 
    -- If column doesn't exist, this will be handled at runtime
    null as return_reason_code
    
  from source_with_metadata
),

-- 2) Enforce key constraints and data quality
filtered as (
  select *
  from normalized
  where return_id is not null
    and order_id is not null
    and product_id is not null
    and return_ts is not null
    and qty > 0
    and reason is not null
),

-- 3) Keep only returns with valid order_id and product_id
with_valid_orders as (
  select f.*
  from filtered f
  where exists (
    select 1
    from {{ ref('stg_orders_header') }} oh
    where oh.order_id = f.order_id
  )
),

with_valid_products as (
  select w.*
  from with_valid_orders w
  where exists (
    select 1
    from {{ ref('stg_products') }} p
    where p.product_id = w.product_id
  )
),

-- 4) Deduplicate on return_id
deduplicated as (
  select * from (
    select *,
      row_number() over (
        partition by return_id 
        order by ingestion_ts desc
      ) as rn
    from with_valid_products
  )
  where rn = 1
)

select
  -- Base schema columns (returns_day1_schema)
  return_id,
  order_id,
  product_id,
  return_ts,
  qty,
  reason,
  
  -- Schema evolution columns (conditionally included)
  return_reason_code,
  
  -- Metadata columns 
  ingestion_ts,
  src_filename,
  src_row_hash
  
from deduplicated
