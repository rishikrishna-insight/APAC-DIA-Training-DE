{{ config(
    materialized='incremental',
    unique_key=['order_id', 'line_number'],
    on_schema_change='merge',
    tags=['fact', 'silver', 'sales']
) }}

-- Fact Table: Sales transactions at order line grain
-- Purpose: Complete sales analysis combining orders, products, customers, and stores
-- Grain: One row per order line
-- Incremental: Only processes new/updated records based on ingestion_ts

with order_items as (
    select * from {{ ref('int_order_items') }}
    
    {% if is_incremental() %}
    -- Only process new records since last run
    where ingestion_ts > (select max(ingestion_ts) from {{ this }})
    {% endif %}
),

customers as (
    select
        customer_id,
        natural_key as customer_key,
        first_name,
        last_name,
        email,
        city as customer_city,
        state_region as customer_state,
        country_code as customer_country,
        is_vip,
        birth_date,
        join_ts as customer_join_ts
    from {{ ref('stg_customers') }}
),

stores as (
    select
        store_id,
        store_code,
        name as store_name,
        channel as store_channel,
        region as store_region,
        state as store_state,
        latitude as store_latitude,
        longitude as store_longitude
    from {{ ref('stg_stores') }}
),

-- Build complete fact table with all dimensions
final as (
    select
        -- Primary Keys
        oi.order_id,
        oi.line_number,
        
        -- Foreign Keys (Dimension references)
        oi.customer_id,
        oi.store_id,
        oi.product_id,
        
        -- Degenerate Dimensions (identifiers that don't warrant their own dimension table)
        oi.sku,
        oi.channel,
        oi.payment_method,
        oi.coupon_code,
        
        -- Date/Time Dimensions
        oi.order_ts,
        oi.order_dt_local,
        cast(oi.order_ts as date) as order_date,
        extract(year from oi.order_ts) as order_year,
        extract(month from oi.order_ts) as order_month,
        extract(day from oi.order_ts) as order_day,
        extract(hour from oi.order_ts) as order_hour,
        dayname(oi.order_ts) as order_day_of_week,
        
        -- Product Attributes (denormalized for faster queries)
        oi.product_name,
        oi.category as product_category,
        oi.subcategory as product_subcategory,
        oi.is_discontinued as product_is_discontinued,
        
        -- Customer key
        c.customer_key,
        
        -- Calculate customer age at order time
        date_diff('year', c.birth_date, oi.order_dt_local) as customer_age_at_order,
        
        -- Calculate days since customer joined
        date_diff('day', cast(c.customer_join_ts as date), oi.order_dt_local) as days_since_customer_joined,
        
        -- Store Attributes 
        s.store_code,
        s.store_name,
        
        -- Quantity Measures
        oi.qty as quantity,
        
        -- Amount Measures (all in decimal for precision)
        oi.unit_price,
        oi.line_gross_amount,
        oi.line_discount_amount,
        oi.line_net_amount,
        oi.line_tax_amount,
        oi.line_total_amount,
        
        -- Percentage Measures
        oi.line_discount_pct as discount_percentage,
        oi.tax_pct as tax_percentage,
        
        -- Calculated Metrics
        case 
            when oi.line_discount_pct > 0 then true 
            else false 
        end as has_discount,
        
        case 
            when oi.coupon_code is not null and oi.coupon_code != '' then true 
            else false 
        end as used_coupon,
        
        -- Metadata
        oi.ingestion_ts,
        current_timestamp as silver_created_at
        
    from order_items oi
    left join customers c on oi.customer_id = c.customer_id
    left join stores s on oi.store_id = s.store_id
)

select * from final
