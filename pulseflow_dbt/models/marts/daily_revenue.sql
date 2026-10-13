SELECT
    DATE(o.created_at) AS order_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity) AS total_items,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM {{ source('public', 'orders') }} o
JOIN {{ source('public', 'order_items') }} oi
    ON o.order_id = oi.order_id
WHERE o.status NOT IN ('CANCELLED', 'CANCELED')
GROUP BY DATE(o.created_at)