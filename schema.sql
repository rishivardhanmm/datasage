CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    name TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT,
    product_name TEXT,
    revenue FLOAT,
    order_date DATE
);

TRUNCATE TABLE orders, customers RESTART IDENTITY;

INSERT INTO customers (name, country) VALUES
('John', 'UK'),
('Alice', 'UK'),
('Emma', 'UK'),
('Oliver', 'UK'),
('Amelia', 'UK'),
('James', 'UK'),
('Sophia', 'UK'),
('Harry', 'UK'),
('Bob', 'India'),
('Priya', 'India'),
('Arjun', 'India'),
('Neha', 'India'),
('Rahul', 'India'),
('Noah', 'USA'),
('Ava', 'USA'),
('Liam', 'USA'),
('Mia', 'USA'),
('Lukas', 'Germany'),
('Hannah', 'Germany'),
('Felix', 'Germany'),
('Omar', 'UAE'),
('Layla', 'UAE'),
('Ethan', 'Canada'),
('Isla', 'Australia'),
('Mason', 'Singapore'),
('Chloe', 'France');

WITH product_catalog (product_idx, product_name, base_revenue) AS (
    VALUES
        (1, 'Laptop', 1200.00),
        (2, 'Phone', 850.00),
        (3, 'Tablet', 600.00),
        (4, 'Monitor', 420.00),
        (5, 'Headphones', 180.00),
        (6, 'Keyboard', 130.00),
        (7, 'Mouse', 75.00),
        (8, 'Printer', 320.00),
        (9, 'Camera', 950.00),
        (10, 'Smartwatch', 400.00)
),
generated_orders AS (
    SELECT
        s AS seq,
        CASE
            WHEN s % 12 BETWEEN 0 AND 4 THEN ((s - 1) % 8) + 1
            WHEN s % 12 BETWEEN 5 AND 7 THEN ((s - 1) % 5) + 9
            WHEN s % 12 BETWEEN 8 AND 9 THEN ((s - 1) % 4) + 14
            WHEN s % 12 = 10 THEN ((s - 1) % 3) + 18
            ELSE ((s - 1) % 6) + 21
        END AS customer_id,
        CASE
            WHEN s % 18 BETWEEN 0 AND 4 THEN 1
            WHEN s % 18 BETWEEN 5 AND 7 THEN 2
            WHEN s % 18 BETWEEN 8 AND 9 THEN 3
            WHEN s % 18 = 10 THEN 4
            WHEN s % 18 = 11 THEN 8
            WHEN s % 18 = 12 THEN 9
            WHEN s % 18 = 13 THEN 10
            WHEN s % 18 = 14 THEN 5
            WHEN s % 18 = 15 THEN 6
            WHEN s % 18 = 16 THEN 7
            ELSE 9
        END AS product_idx,
        DATE '2026-01-01' + (((s * 5) + (s / 4)) % 90) AS order_date
    FROM generate_series(1, 144) AS s
)
INSERT INTO orders (customer_id, product_name, revenue, order_date)
SELECT
    generated_orders.customer_id,
    product_catalog.product_name,
    ROUND(
        (
            product_catalog.base_revenue
            * CASE
                WHEN generated_orders.customer_id BETWEEN 1 AND 8 THEN 1.12
                WHEN generated_orders.customer_id BETWEEN 9 AND 13 THEN 0.88
                WHEN generated_orders.customer_id BETWEEN 14 AND 17 THEN 1.00
                WHEN generated_orders.customer_id BETWEEN 18 AND 20 THEN 0.96
                ELSE 0.92
            END
            * CASE generated_orders.seq % 5
                WHEN 0 THEN 0.94
                WHEN 1 THEN 1.00
                WHEN 2 THEN 1.06
                WHEN 3 THEN 1.12
                ELSE 0.98
            END
            * CASE
                WHEN EXTRACT(MONTH FROM generated_orders.order_date) = 2
                    AND product_catalog.product_name IN ('Laptop', 'Phone', 'Camera')
                    THEN 1.08
                WHEN EXTRACT(MONTH FROM generated_orders.order_date) = 3
                    AND product_catalog.product_name IN ('Tablet', 'Monitor', 'Printer')
                    THEN 1.05
                ELSE 1.00
            END
        )::numeric,
        2
    )::float,
    generated_orders.order_date
FROM generated_orders
JOIN product_catalog ON product_catalog.product_idx = generated_orders.product_idx
ORDER BY generated_orders.seq;
