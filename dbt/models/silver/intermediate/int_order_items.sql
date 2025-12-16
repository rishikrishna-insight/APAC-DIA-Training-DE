{{ config(
    materialized='table',
    tags=['intermediate', 'silver']
) }}

-- Intermediate model: Calculate order line items with business metrics
-- Purpose: Enrich order lines with calculated amounts before joining to fact table
-- Grain: One row per order line (same as stg_orders_lines)

with order_lines as (
    select * from {{ ref('stg_orders_lines') }}
),

order_header as (
    select 
        order_id,
        order_ts,
        order_dt_local,
        customer_id,
        store_id,
        channel,
        payment_method,
        coupon_code,
        shipping_fee,
        currency
    from {{ ref('stg_orders_header') }}
),

products as (
    select
        product_id,
        sku,
        name as product_name,
        category,
        subcategory,
        current_price,
        is_discontinued
    from {{ ref('stg_products') }}
),

-- Calculate line-level business metrics
enriched_lines as (
    select
        -- Order identifiers
        ol.order_id,
        ol.line_number,
        
        -- Product info
        ol.product_id,
        p.sku,
        p.product_name,
        p.category,
        p.subcategory,
        p.is_discontinued,
        
        -- Order context
        oh.order_ts,
        oh.order_dt_local,
        oh.customer_id,
        oh.store_id,
        oh.channel,
        oh.payment_method,
        oh.coupon_code,
        
        -- Line item quantities and prices
        ol.qty,
        ol.unit_price,
        ol.line_discount_pct,
        ol.tax_pct,
        
        -- Calculate business metrics
        -- Gross amount = quantity * unit price
        (ol.qty * ol.unit_price) as line_gross_amount,
        
        -- Discount amount in dollars
        (ol.qty * ol.unit_price * ol.line_discount_pct / 100) as line_discount_amount,
        
        -- Net amount after discount
        (ol.qty * ol.unit_price * (1 - ol.line_discount_pct / 100)) as line_net_amount,
        
        -- Tax amount in dollars
        (ol.qty * ol.unit_price * (1 - ol.line_discount_pct / 100) * ol.tax_pct / 100) as line_tax_amount,
        
        -- Final line total including tax
        (ol.qty * ol.unit_price * (1 - ol.line_discount_pct / 100) * (1 + ol.tax_pct / 100)) as line_total_amount,
        
        -- Metadata
        ol.ingestion_ts
        
    from order_lines ol
    inner join order_header oh on ol.order_id = oh.order_id
    left join products p on ol.product_id = p.product_id  -- Left join to keep orders even if product missing
)

select * from enriched_lines
