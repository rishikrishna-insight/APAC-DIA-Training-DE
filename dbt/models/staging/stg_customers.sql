{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_customers_parquet
),

-- 1) Normalize to new typed columns; DO NOT reuse original names yet
normalized as (
  select
    cast(customer_id as bigint)                                    as customer_id,
    cast(natural_key as varchar)                                   as natural_key,

    lower(trim(first_name))                                        as first_name,
    lower(trim(last_name))                                         as last_name,

    {% set email_regex = "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$" %}
    case
      when email is null or trim(email) = '' then null
      when regexp_full_match(lower(trim(email)), '{{ email_regex }}') then lower(trim(email))
      else null
    end                                                            as email,

    cast(phone as varchar)                                         as phone,
    lower(trim(address_line1))                                     as address_line1,
    nullif(lower(trim(address_line2)), '')                         as address_line2,
    lower(trim(city))                                              as city,
    
    lower(trim(state_region))                                      as state_region,
    lower(trim(postcode))                                          as postcode,
    upper(trim(country_code))                                      as country_code,
    case 
      when latitude between -90 and 90 then cast(latitude as double)
      else null 
    end                                                            as latitude,
    case 
      when longitude between -180 and 180 then cast(longitude as double)
      else null 
    end                                                            as longitude, 
    cast(birth_date as date)                                       as birth_date,
    {{ convert_adelaide_to_utc('join_ts') }}                       as join_ts,
    cast(is_vip as boolean)                                        as is_vip,

    cast(gdpr_consent as boolean)                                  as gdpr_consent,
    
    -- Bronze metadata columns
    {{ current_utc_timestamp() }}                                  as ingestion_ts,
    cast('customers.parquet' as varchar)                           as src_filename,
    cast(src_row_hash as varchar)                                  as src_row_hash
  from src
),

-- 2) Enforce key using ONLY the numeric column
filtered as (
  select *
  from normalized
  where customer_id is not null
),

-- 3) Deduplicate on natural_key; order by join_ts_utc then numeric id
dedup as (
  select *
  from (
    select
      f.*,
      row_number() over (
        partition by natural_key
        order by coalesce(join_ts, timestamp '1970-01-01') desc,
                 customer_id desc
      ) as _rn
    from filtered f
  ) t
  where _rn = 1
)

-- 4) Final projection with canonical names (match contract)
select
  customer_id,
  natural_key,
  first_name,
  last_name,
  email,
  phone,
  address_line1,
  address_line2,
  city,
  state_region,
  postcode,
  country_code,
  latitude,
  longitude,
  birth_date,
  join_ts,
  is_vip,
  gdpr_consent,
  ingestion_ts,
  src_filename,
  src_row_hash
from dedup
