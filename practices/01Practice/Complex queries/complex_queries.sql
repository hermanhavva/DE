-- Load data into tables from csv

CREATE TABLE customers AS SELECT * FROM read_csv_auto('path_to_your_datasets/datasets/customers.csv');
CREATE TABLE products AS SELECT * FROM read_csv_auto('path_to_your_datasets/datasets/products.csv');
CREATE TABLE regions AS SELECT * FROM read_csv_auto('path_to_your_datasets/datasets/regions.csv');
CREATE TABLE sales AS SELECT * FROM read_csv_auto('path_to_your_datasets/datasets/sales.csv');
CREATE TABLE transactions AS SELECT * FROM read_csv_auto('path_to_your_datasets/datasets/transactions.csv');

-- Query 1
WITH product_sales AS (
    SELECT
        p.product_id,
        p.product_name,
        s.sale_date,
        s.quantity_sold,
        p.price,
        s.quantity_sold * p.price AS total_sale_amount
    FROM products p
    JOIN sales s ON p.product_id = s.product_id
),
customer_transactions AS (
    SELECT
        t.transaction_id,
        t.customer_id,
        c.customer_name,
        c.region,
        t.amount
    FROM transactions t
    JOIN customers c ON t.customer_id = c.customer_id
),
region_summary AS (
    SELECT
        c.region,
        r.region_manager,
        COUNT(t.transaction_id) AS total_transactions,
        SUM(t.amount) AS total_revenue
    FROM customer_transactions t
    JOIN customers c ON t.customer_id = c.customer_id
    JOIN regions r ON c.region = r.region
    GROUP BY c.region, r.region_manager
)
SELECT
    ps.product_name,
    ps.sale_date,
    ps.total_sale_amount,
    ct.customer_name,
    rs.region_manager,
    rs.total_revenue
FROM product_sales ps
JOIN customer_transactions ct ON ps.product_id = ct.customer_id
JOIN region_summary rs ON ct.region = rs.region;


-- Query 2
WITH sales_summary AS (
    SELECT
        s.product_id,
        p.product_name,
        SUM(s.quantity_sold) AS total_quantity_sold,
        SUM(s.quantity_sold * p.price) AS total_sales_revenue
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY s.product_id, p.product_name
),
customer_purchases AS (
    SELECT
        t.customer_id,
        c.customer_name,
        SUM(t.amount) AS total_spent,
        COUNT(t.transaction_id) AS total_transactions
    FROM transactions t
    JOIN customers c ON t.customer_id = c.customer_id
    GROUP BY t.customer_id, c.customer_name
),
regional_performance AS (
    SELECT
        c.region,
        r.region_manager,
        SUM(t.amount) AS regional_revenue,
        COUNT(DISTINCT t.customer_id) AS unique_customers
    FROM transactions t
    JOIN customers c ON t.customer_id = c.customer_id
    JOIN regions r ON c.region = r.region
    GROUP BY c.region, r.region_manager
)
SELECT
    ss.product_name,
    ss.total_quantity_sold,
    ss.total_sales_revenue,
    cp.customer_name,
    cp.total_spent,
    rp.region_manager,
    rp.regional_revenue,
    rp.unique_customers
FROM sales_summary ss
LEFT JOIN customer_purchases cp ON cp.total_spent > 500
LEFT JOIN regional_performance rp ON rp.regional_revenue > 1000
WHERE ss.total_sales_revenue > 100
ORDER BY ss.total_sales_revenue DESC, cp.total_spent DESC;
