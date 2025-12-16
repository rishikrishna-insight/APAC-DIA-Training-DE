{% snapshot products_snapshot %}
{{
  config(
    target_schema='snapshot',
    unique_key='product_id',
    strategy='check',
    check_cols=['name','category','subcategory','current_price','currency','is_discontinued']
  )
}}

-- Track product changes over time (price changes, discontinuation, etc.)
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
  discontinued_dt
from {{ ref('stg_products') }}

{% endsnapshot %}
