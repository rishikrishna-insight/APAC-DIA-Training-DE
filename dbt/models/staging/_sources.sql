{{ config(materialized='view') }}

-- External views pointing at Bronze Parquet files in samples directory
{% set lake_root = 'c:/Users/rsureshk/Documents/APAC-DIA-Training-DE/lake/bronze' %}

{% if execute %}
  {% do run_query("create schema if not exists bronze") %}

  -- Parquet views pointing to samples directory - start with customers only
  {% do run_query("create or replace view bronze_customers_parquet as select * from read_parquet('" ~ lake_root ~ "/parquet/samples/customers/*.parquet')") %}
  {% do run_query("create or replace view bronze_products_parquet as select * from read_parquet('" ~ lake_root ~ "/parquet/samples/products/*.parquet')") %}
  {% do run_query("create or replace view bronze_stores_parquet as select * from read_parquet('" ~ lake_root ~ "/parquet/samples/stores/*.parquet')") %}
  {% do run_query("create or replace view bronze_suppliers_parquet as select * from read_parquet('" ~ lake_root ~ "/parquet/samples/suppliers/*.parquet')") %}
  {% do run_query("create or replace view bronze_orders_header_parquet as select * from read_parquet('" ~ lake_root ~ "/parquet/samples/orders_header/**/*.parquet')") %}
  {% do run_query("create or replace view bronze_orders_lines_parquet as select * from read_parquet('" ~ lake_root ~ "/parquet/samples/orders_lines/**/*.parquet')") %}
  {% do run_query("create or replace view bronze_exchange_rates_parquet as select * from read_parquet('" ~ lake_root ~ "/parquet/samples/exchange_rates/*.parquet')") %}
  {% do run_query("create or replace view bronze_shipments_parquet as select * from read_parquet('" ~ lake_root ~ "/parquet/samples/shipments/*.parquet')") %}
  {% do run_query("create or replace view bronze_returns_parquet as select * from read_parquet('" ~ lake_root ~ "/parquet/samples/returns/**/*.parquet')") %}
  {% do run_query("create or replace view bronze_events_parquet as select * from read_parquet('" ~ lake_root ~ "/parquet/samples/events/**/*.parquet')") %}
{% endif %}

-- Individual source views - each table has its own schema
-- This model creates the bronze views but doesn't return unified data
-- Each staging model will reference the specific bronze view directly

select 1 as setup_complete
