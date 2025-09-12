{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_suppliers_parquet
),

-- 1) Clean and cast columns based on suppliers schema
normalized as (
  select
    cast(supplier_id as bigint)                                    as supplier_id,
    cast(supplier_code as varchar)                                 as supplier_code,
    
    trim(name)                                                     as name,
    upper(trim(country_code))                                      as country_code,
    
    cast(lead_time_days as int)                                    as lead_time_days,
    cast(preferred as boolean)                                     as preferred,
    
    -- Bronze metadata columns  
    {{ current_utc_timestamp() }}                                  as ingestion_ts,
    cast('suppliers.parquet' as varchar)                           as src_filename,
    cast(src_row_hash as varchar)                                  as src_row_hash
  from src
),

-- 2) Enforce key constraints
filtered as (
  select *
  from normalized
  where supplier_id is not null
    and supplier_code is not null
),

-- 3) Deduplicate on supplier_code (natural key)
dedup as (
  select *
  from (
    select
      f.*,
      row_number() over (
        partition by supplier_code
        order by supplier_id desc
      ) as _rn
    from filtered f
  ) t
  where _rn = 1
)

-- 4) Final projection
select
  supplier_id,
  supplier_code,
  name,
  country_code,
  lead_time_days,
  preferred,
  ingestion_ts,
  src_filename,
  src_row_hash
from dedup
