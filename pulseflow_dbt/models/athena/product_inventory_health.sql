{{ config(materialized='table') }}

SELECT
    product_id,
    name AS product_name,
    category,
    price,
    stock,
    inventory_value,
    event_timestamp,
    operation,
    gold_processed_at,
    CASE
        WHEN stock = 0 THEN 'OUT_OF_STOCK'
        WHEN stock > 0 THEN 'IN_STOCK'
        ELSE 'INVALID_STOCK'
    END AS inventory_status
FROM {{ source('strataflow_gold', 'products_current') }}
