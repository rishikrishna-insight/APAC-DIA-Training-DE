{{ config(materialized='table', contract={'enforced': true}) }}

-- Staging model for exchange rates from Bronze parquet files
-- Source: schemas.exchange_rates_schema - treat schema column names as source of truth

-- Exact columns from exchange_rates_schema:
-- date (date32), currency (string), rate_to_aud (decimal128(18, 8))

with src as (
  select * from bronze_exchange_rates_parquet
),

-- 1) Clean and standardize data types according to schema
normalized as (
  select
    -- Schema columns - exact mapping
    cast(date as date)                                             as date,
    cast(upper(trim(currency)) as varchar)                         as currency,
    cast(rate_to_aud as decimal(18,8))                             as rate_to_aud,
    
    -- Bronze metadata columns  
    {{ current_utc_timestamp() }}                                  as ingestion_ts,
    cast('exchange_rates.parquet' as varchar)                     as src_filename,
    cast(src_row_hash as varchar)                                 as src_row_hash
  from src
),

-- 2) Enforce key constraints and data quality
filtered as (
  select *
  from normalized
  where date is not null
    and currency is not null
    and rate_to_aud > 0
),

-- 3) Deduplicate on composite key (date, currency)
deduplicated as (
  select * from (
    select *,
      row_number() over (
        partition by date, currency 
        order by ingestion_ts desc
      ) as rn
    from filtered
  )
  where rn = 1
)

select
  -- Schema columns only - exact mapping from exchange_rates_schema
  date,
  currency,
  rate_to_aud,
  
  -- Metadata columns (exceptions allowed)
  ingestion_ts,
  src_filename,
  src_row_hash
  
from deduplicated
