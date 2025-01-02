/*

ROW_NUMBER(): Assigns a unique sequential number to each row within a partition.
LEAD(): Provides the value of the next row (relative to the current row).
LAG(): Provides the value of the previous row (relative to the current row).
QUALIFY: The QUALIFY keyword is used to filter the result of a window function after the window function has been applied.

*/
-- Query 1: Rank sales for each product

/*
 * This query assigns a row number to each sale within each product_id
 * based on the descending order of sale_date. The most recent sale for each product will have a row_num of 1.
 */


SELECT
    sale_id,
    product_id,
    sale_date,
    quantity_sold,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC) AS row_num
FROM sales
ORDER BY product_id, row_num;


-- Query 2: Show the next quantity sold for each product

/*
 *This query shows the quantity sold in the next sale for each product, ordered by the sale_date.
 *If there is no subsequent sale, the next_quantity_sold will be NULL.
 */

SELECT
    sale_id,
    product_id,
    sale_date,
    quantity_sold,
    LEAD(quantity_sold) OVER (PARTITION BY product_id ORDER BY sale_date) AS next_quantity_sold
FROM sales;

-- Query 3: Show the previous sale date for each product

/*
 * This query returns the sale_date for the previous sale of each product, helping to track past sales events for each product.
 */

SELECT
    sale_id,
    product_id,
    sale_date,
    LAG(sale_date) OVER (PARTITION BY product_id ORDER BY sale_date) AS prev_sale_date
FROM sales;


-- QUALIFY
SELECT
    sale_id,
    product_id,
    sale_date,
    quantity_sold,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC) AS row_num
FROM sales
QUALIFY row_num = 1;


SELECT
    sale_id,
    product_id,
    sale_date,
    quantity_sold
FROM sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC) = 1;
