{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_stores_parquet
),

-- 1) Clean and cast columns based on stores schema
normalized as (
  select
    cast(store_id as bigint)                                       as store_id,
    cast(store_code as varchar)                                    as store_code,
    
    trim(name)                                                     as name,
    trim(channel)                                                  as channel,
    trim(region)                                                   as region,
    trim(state)                                                    as state,
    
    -- Validate and clean geospatial data
    case 
      when latitude between -90 and 90 then cast(latitude as double)
      else null 
    end                                                            as latitude,
    case 
      when longitude between -180 and 180 then cast(longitude as double)
      else null 
    end                                                            as longitude,
    
    cast(open_dt as date)                                          as open_dt,
    cast(close_dt as date)                                         as close_dt,
    
    -- Bronze metadata columns  
    {{ current_utc_timestamp() }}                                  as ingestion_ts,
    cast('stores.parquet' as varchar)                              as src_filename,
    cast(src_row_hash as varchar)                                  as src_row_hash
  from src
),

-- 2) Enforce key constraints
filtered as (
  select *
  from normalized
  where store_id is not null
    and store_code is not null
),

-- 3) Deduplicate on store_code (natural key)
dedup as (
  select *
  from (
    select
      f.*,
      row_number() over (
        partition by store_code
        order by coalesce(open_dt, date '1970-01-01') desc,
                 store_id desc
      ) as _rn
    from filtered f
  ) t
  where _rn = 1
)

-- 4) Final projection
select
  store_id,
  store_code,
  name,
  channel,
  region,
  state,
  latitude,
  longitude,
  open_dt,
  close_dt,
  ingestion_ts,
  src_filename,
  src_row_hash
from dedup
