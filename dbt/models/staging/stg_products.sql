{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_products_parquet
),

-- 1) Clean and cast columns based on products schema
normalized as (
  select
    cast(product_id as bigint)                                     as product_id,
    cast(sku as varchar)                                           as sku,
    
    trim(name)                                                     as name,
    trim(category)                                                 as category,
    trim(subcategory)                                              as subcategory,
    
    cast(current_price as decimal(12,4))                           as current_price,
    trim(currency)                                                 as currency,
    
    cast(is_discontinued as boolean)                               as is_discontinued,
    cast(introduced_dt as date)                                    as introduced_dt,
    cast(discontinued_dt as date)                                  as discontinued_dt,
    
    -- Bronze metadata columns  
    {{ current_utc_timestamp() }}                                  as ingestion_ts,
    cast('products.parquet' as varchar)                            as src_filename,
    cast(src_row_hash as varchar)                                  as src_row_hash
  from src
),

-- 2) Enforce key constraints
filtered as (
  select *
  from normalized
  where product_id is not null
    and sku is not null
),

-- 3) Deduplicate on sku (natural key)
dedup as (
  select *
  from (
    select
      f.*,
      row_number() over (
        partition by sku
        order by coalesce(introduced_dt, date '1970-01-01') desc,
                 product_id desc
      ) as _rn
    from filtered f
  ) t
  where _rn = 1
)

-- 4) Final projection
select
  product_id,
  sku,
  name,
  category,
  subcategory,
  current_price,
  currency,
  is_discontinued,
  introduced_dt,
  discontinued_dt,
  ingestion_ts,
  src_filename,
  src_row_hash
from dedup