# Exercise 01 Raw Schemas Review Notes

## Assumptions
- All IDs (e.g., `customer_id`, `product_id`) are unique (unless controlled anomaly introduced) and serve as primary keys.
- Not all fields are required.
- Timestamps (e.g., `join_ts`, `order_ts`) are in UTC.
- Decimal fields are used for monetary values and percentages.

## Foreign Key Relationship Assumptions
- `orders_header.customer_id` references `customers.customer_id`.
- `orders_header.store_id` references `stores.store_id`.
- `orders_lines.order_id` references `orders_header.order_id`.
- `orders_lines.product_id` references `products.product_id`.
- Foreign key relationships are implied by field naming but are not enforced at the schema level.