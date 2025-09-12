{{ config(
    materialized='incremental',
    unique_key=['order_id', 'line_number'],
    contract={'enforced': true},
    on_schema_change='fail'
) }}

-- Incremental staging model for order lines from Bronze parquet files
-- High volume table (3-4M+ rows) - using incremental for performance
-- Source: schemas.orders_lines_schema - treat schema column names as source of truth

-- Exact columns from orders_lines_schema:
-- order_id (int64), line_number (int32), product_id (int64), qty (int32), 
-- unit_price (decimal128(12, 4)), line_discount_pct (decimal128(5, 4)), tax_pct (decimal128(5, 4))

with src as (
  select * from bronze_orders_lines_parquet
  
  {% if is_incremental() %}
    -- Incremental logic: only process new/updated records
    where ingestion_ts > (select max(ingestion_ts) from {{ this }})
  {% endif %}
),

-- 1) Clean and standardize data types according to schema
normalized as (
  select
    -- Schema columns - exact mapping
    cast(order_id as bigint)                                       as order_id,
    cast(line_number as integer)                                   as line_number,
    cast(product_id as bigint)                                     as product_id,
    cast(qty as integer)                                           as qty,
    cast(unit_price as decimal(12,4))                              as unit_price,
    cast(line_discount_pct as decimal(5,4))                        as line_discount_pct,
    cast(tax_pct as decimal(5,4))                                  as tax_pct,
    
    -- Bronze metadata columns  
    {{ current_utc_timestamp() }}                                  as ingestion_ts,
    cast('orders_lines.parquet' as varchar)                       as src_filename,
    cast(src_row_hash as varchar)                                 as src_row_hash
  from src
),

-- 2) Enforce key constraints and data quality
filtered as (
  select *
  from normalized
  where order_id is not null
    and line_number is not null
    and product_id is not null
    and qty > 0
    and unit_price >= 0
),

-- 3) Keep only lines with valid order_id and product_id
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

-- 4) Deduplicate on composite key (order_id, line_number)
deduplicated as (
  select * from (
    select *,
      row_number() over (
        partition by order_id, line_number 
        order by ingestion_ts desc
      ) as rn
    from with_valid_products
  )
  where rn = 1
)

select
  -- Schema columns only - exact mapping from orders_lines_schema
  order_id,
  line_number,
  product_id,
  qty,
  unit_price,
  line_discount_pct,
  tax_pct,
  
  -- Metadata columns (exceptions allowed)
  ingestion_ts,
  src_filename,
  src_row_hash
  
from deduplicated
