SELECT
    p.product_id,
    p.name AS product_name,
    p.category,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue,
    COUNT(DISTINCT oi.order_id) AS number_of_orders
FROM {{ source('public', 'products') }} p
JOIN {{ source('public', 'order_items') }} oi
    ON p.product_id = oi.product_id
JOIN {{ source('public', 'orders') }} o
    ON oi.order_id = o.order_id
WHERE o.status NOT IN ('CANCELLED', 'CANCELED')
GROUP BY
    p.product_id,
    p.name,
    p.category