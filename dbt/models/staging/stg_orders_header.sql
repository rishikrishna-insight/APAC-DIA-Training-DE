{{ config(
    materialized='incremental',
    unique_key='order_id',
    contract={'enforced': true},
    on_schema_change='fail'
) }}

-- Incremental staging model for orders header
-- High volume table (1M+ rows) - using incremental for performance

with src as (
  select * from bronze_orders_header_parquet
  
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
    {{ convert_adelaide_to_utc('order_ts') }}                      as order_ts,
    cast(order_dt_local as date)                                   as order_dt_local,
    cast(customer_id as bigint)                                    as customer_id,
    cast(store_id as bigint)                                       as store_id,
    cast(channel as varchar)                                       as channel,
    cast(payment_method as varchar)                                as payment_method,
    cast(coupon_code as varchar)                                   as coupon_code,
    cast(shipping_fee as decimal(12,2))                            as shipping_fee,
    cast(currency as varchar)                                      as currency,
    
    -- Bronze metadata columns  
    {{ current_utc_timestamp() }}                                  as ingestion_ts,
    cast('orders_header.parquet' as varchar)                      as src_filename,
    cast(src_row_hash as varchar)                                 as src_row_hash
  from src
),

-- 2) Enforce key constraints and data quality
filtered as (
  select *
  from normalized
  where order_id is not null
    and order_ts is not null
    and order_dt_local is not null
),

-- 3) Keep only orders with valid customer_id and store_id
with_valid_customers as (
  select f.*
  from filtered f
  where exists (
    select 1
    from {{ ref('stg_customers') }} c
    where c.customer_id = f.customer_id
  )
),

with_valid_stores as (
  select w.*
  from with_valid_customers w
  where exists (
    select 1
    from {{ ref('stg_stores') }} s
    where s.store_id = w.store_id
  )
),

-- 4) Deduplicate on order_id (handle 0.05% duplicate order_ids mentioned in docs)
dedup as (
  select *
  from (
    select
      w.*,
      row_number() over (
        partition by order_id
        order by order_ts desc,
                 ingestion_ts desc
      ) as _rn
    from with_valid_stores w
  ) t
  where _rn = 1
)

select
  -- Schema columns only - exact mapping from orders_header_schema
  order_id,
  order_ts,
  order_dt_local,
  customer_id,
  store_id,
  channel,
  payment_method,
  coupon_code,
  shipping_fee,
  currency,
  
  -- Metadata columns (exceptions allowed)
  ingestion_ts,
  src_filename,
  src_row_hash
  
from dedup
