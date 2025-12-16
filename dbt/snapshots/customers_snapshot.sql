{% snapshot customers_snapshot %}
{{
  config(
    target_schema='snapshot',
    unique_key='customer_id',
    strategy='check',
    check_cols=['email','phone','address_line1','address_line2','city','state_region','postcode','is_vip']
  )
}}

-- Track customer changes over time (address moves, VIP status changes, contact info updates)
-- Uses cleaned data from staging layer
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
  is_vip,
  gdpr_consent
from {{ ref('stg_customers') }}

{% endsnapshot %}
