-- Task 4.1: Create Hive Table
CREATE TABLE IF NOT EXISTS orders (
    Order_ID STRING,
    Customer_ID STRING,
    Order_Date STRING,
    City STRING,
    Product_Category STRING,
    Quantity INT,
    Unit_Price DOUBLE,
    Discount_Amount DOUBLE,
    Total_Amount DOUBLE,
    Payment_Method STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Task 4.1: Load Dataset from HDFS
LOAD DATA INPATH '/ecommerce/orders.csv' OVERWRITE INTO TABLE orders;

-- Task 4.2: Query 1 - Total Number of Orders
SELECT COUNT(*) AS total_orders 
FROM orders;

-- Task 4.3: Query 2 - Total Sales by Category
SELECT Product_Category, ROUND(SUM(Total_Amount), 2) AS total_sales
FROM orders
GROUP BY Product_Category
ORDER BY total_sales DESC;

-- Task 4.4: Query 3 - Orders by City
SELECT City, COUNT(*) AS number_of_orders
FROM orders
GROUP BY City
ORDER BY number_of_orders DESC;

-- Task 4.5: Query 4 - Most Frequently Used Payment Method
SELECT Payment_Method, COUNT(*) AS usage_count
FROM orders
GROUP BY Payment_Method
ORDER BY usage_count DESC
LIMIT 1;
