SELECT
    c.customer_id,
    c.name AS customer_name,
    c.email,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS total_spent,
    COALESCE(AVG(o.total_amount), 0) AS average_order_value,
    MAX(o.created_at) AS last_order_date
FROM {{ source('public', 'customers') }} c
LEFT JOIN {{ source('public', 'orders') }} o
    ON c.customer_id = o.customer_id
    AND o.status NOT IN ('CANCELLED', 'CANCELED')
GROUP BY
    c.customer_id,
    c.name,
    c.email