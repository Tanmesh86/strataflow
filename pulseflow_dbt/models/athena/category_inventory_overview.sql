{{ config(materialized='table') }}

SELECT
    category,
    product_count,
    total_stock_units,
    inventory_value,
    avg_product_price,
    gold_processed_at
FROM {{ source('strataflow_gold', 'category_inventory_metrics') }}
