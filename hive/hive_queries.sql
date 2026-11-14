-- ====================================================================
-- Big Data E-Commerce Analytics - Apache Hive SQL Queries
-- Course: Big Data Technologies
-- Execution Environment: Apache Hive 2.3.2 via Beeline (Hadoop 3.2.1)
-- ====================================================================

-- --------------------------------------------------------------------
-- 1. Create External Table Mapped to HDFS
-- --------------------------------------------------------------------
-- Points directly to the HDFS dataset located at /ecommerce/input
-- Skips the CSV header row during query parsing
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_orders (
    order_id STRING,
    customer_id STRING,
    order_date STRING,
    city STRING,
    category STRING,
    quantity INT,
    price DOUBLE,
    discount DOUBLE,
    total_amount DOUBLE,
    payment_method STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/ecommerce/input'
TBLPROPERTIES ("skip.header.line.count"="1");

-- --------------------------------------------------------------------
-- 2. Verify Table Creation & Data Sampling
-- --------------------------------------------------------------------
SELECT * FROM ecommerce_orders LIMIT 5;
SELECT COUNT(*) AS total_orders FROM ecommerce_orders;

-- --------------------------------------------------------------------
-- 3. Analytics Query 1: Total Orders Count by Product Category
-- --------------------------------------------------------------------
-- Evaluates transaction volume across different product categories
SELECT category, COUNT(*) AS total_orders
FROM ecommerce_orders
GROUP BY category;

-- --------------------------------------------------------------------
-- 4. Analytics Query 2: Payment Method Transaction Breakdown
-- --------------------------------------------------------------------
-- Analyzes customer payment preferences across all orders
SELECT payment_method, COUNT(*) AS total_orders
FROM ecommerce_orders
GROUP BY payment_method
ORDER BY total_orders DESC;

-- --------------------------------------------------------------------
-- 5. Analytics Query 3: Total Sales Revenue by Product Category
-- --------------------------------------------------------------------
-- Calculates total financial revenue generated per product category (Quantity * Unit Price)
SELECT category, ROUND(SUM(quantity * price), 2) AS total_sales
FROM ecommerce_orders
GROUP BY category
ORDER BY total_sales DESC;

-- --------------------------------------------------------------------
-- 6. Analytics Query 4: Total Orders by City
-- --------------------------------------------------------------------
-- Evaluates order volume by Turkish metropolitan city
SELECT city, COUNT(*) AS total_orders
FROM ecommerce_orders
GROUP BY city
ORDER BY total_orders DESC;

-- --------------------------------------------------------------------
-- 7. Analytics Query 5: Overall Platform Summary
-- --------------------------------------------------------------------
-- Aggregates total order count and total gross merchandise value
SELECT 
    COUNT(*) AS total_orders, 
    ROUND(SUM(quantity * price), 2) AS total_revenue,
    ROUND(AVG(quantity * price), 2) AS average_order_value
FROM ecommerce_orders;
