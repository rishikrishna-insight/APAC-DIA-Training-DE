# Data Generation Anomalies Summary

This document lists the intentionally injected anomalies in the synthetic datasets, as per `01_raw_schemas.md` requirements.

---

## Customers
**Target Rows:** ~80,000

| Column Name       | Anomaly Description |
|-------------------|--------------------|
| `email`           | ~0.9% malformed emails (`bad_email`) |
| `natural_key`     | ~0.2% duplicate values |
| `phone`           | ~7% null/empty values |
| `address_line1`   | ~7% null/empty values |
| `address_line2`   | ~50% null/empty values |

---

## Products
**Target Rows:** ~25,000

| Column Name         | Anomaly Description |
|---------------------|--------------------|
| `current_price`     | ~0.3% missing or invalid values |
| `discontinued_dt`   | For discontinued products (~20% of dataset): 20% have null/empty discontinued date |
| `is_discontinued`   | ~20% of products marked as discontinued |

---

## Stores
**Target Rows:** ~5,000

| Column Name     | Anomaly Description |
|-----------------|--------------------|
| `store_code`    | ~0.5% duplicate values |
| `latitude`      | ~0.5% impossible values (outside valid range -90 to 90) |
| `longitude`     | ~0.5% impossible values (outside valid range -180 to 180) |

---

## Suppliers
**Target Rows:** ~8,000

| Column Name | Anomaly Description |
|-------------|--------------------|
| *(None)*    | No anomalies injected for this dataset |

---
---

## Orders Header
**Target Rows:** ≥1,000,000

| Column Name    | Anomaly Description |
|----------------|---------------------|
| `customer_id`  | ~1% invalid foreign key values (do not match customers table) |
| `store_id`     | ~1% invalid foreign key values (do not match stores table) |
| `order_id`     | ~0.05% duplicates across partitions |
| `coupon_code`  | ~85% null (empty string) |
| `shipping_fee` | Distribution includes $0.00, low, medium, and high values |

---

## Orders Lines
**Target Rows:** ~3–4,000,000

| Column Name        | Anomaly Description |
|--------------------|---------------------|
| `product_id`       | ~1% invalid foreign key values (do not match products table) |
| `qty`              | ~0.1% zero or negative quantities |
| `unit_price`       | ~0.05% zero price values |
| `line_discount_pct`| ~10% high discount values (50–100%) |
| `tax_pct`          | Random uniform distribution between 5–15% |

---

## Events
**Target Rows:** ~2,000,000

| Column Name / Record | Anomaly Description |
|----------------------|---------------------|
| JSON line            | ~0.05% malformed JSON strings |
| Envelope fields      | ~0.15% missing 1–2 required envelope fields (event_id, event_ts, event_type, user_id, session_id) |

---

## Sensors
**Target Rows:** 5–10,000,000

| Column Name     | Anomaly Description |
|-----------------|---------------------|
| `temperature_c` | ~0.3% extreme values > 999 (impossible) |
| `humidity_pct`  | ~0.3% extreme values > 999 (impossible) |
| `sensor_ts`     | ~0.1% missing timestamps |

---

## Exchange Rates
**Target Rows:** ~1,100 (3 years daily)

| Column Name  | Anomaly Description |
|--------------|---------------------|
| *(None)*     | No anomalies injected for this dataset |

---

## Shipments
**Target Rows:** ~1,000,000

| Column Name     | Anomaly Description |
|-----------------|---------------------|
| `order_id`      | ~1% invalid foreign key values (do not match orders_header table) |
| `delivered_at`  | ~10% null values (in-transit shipments) |
| `delivered_at`  | ~5% late deliveries (> 7 days after shipped_at) |

---

## Returns (v1)
**Target Rows:** ~100,000

| Column Name  | Anomaly Description |
|--------------|---------------------|
| *(None)*     | Base dataset contains only valid references and values |

---

## Returns (v2)
**Target Rows:** Updated from v1

| Column Name          | Anomaly Description |
|----------------------|---------------------|
| `return_reason_code` | Added in v2 (schema evolution) |
| `reason`             | Updated for ~5% of rows (UPSERT) |
| Rows                 | ~2% new rows inserted |
| Rows                 | ~1% rows deleted |

