-- Data Engineering SQL
-- Term Project 1 by Isil Oral
-- November 6, 2024

-- ---------------------------------------------------
-- ANALYTICS
-- Project Plan: Sales Analytics Data Pipeline
-- ---------------------------------------------------
-- Goal:
-- This project aims to create a structured data pipeline for analyzing sales data, customer demographics, 
-- and product performance. This pipeline will transform raw sales, customer, order, and product data 
-- into meaningful insights, particularly for marketing and operational improvements.

-- Analytics Objectives:
-- 1. Identify key revenue contributors by age and gender to guide marketing strategies.
-- 2. Analyze product performance by tracking top-selling products, revenue by product type, 
--    and sales trends over time.
-- 3. Investigate delivery performance by analyzing average delivery times by state, 
--    helping to improve logistics and customer satisfaction.
-- 4. Detect significant increases in product sales through moving average calculations, 
--    identifying potential trends or surges in demand.

-- Drop and create schema
DROP SCHEMA IF EXISTS termproject1;
CREATE SCHEMA termproject1;
USE termproject1;

-- ---------------------------------------------------
-- OPERATIONAL LAYER
-- Step 1: Creating Core Tables
-- ---------------------------------------------------

-- Drop and create 'sales' table to store individual sales records
DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    sales_id INT NOT NULL,                
    order_id INT NOT NULL,                
    product_id INT NOT NULL,              
    price_per_unit DECIMAL(10, 2),        
    quantity INT,                         
    total_price DECIMAL(10, 2),           
    PRIMARY KEY (sales_id)                
);

-- Drop and create 'products' table to store product information
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id INT NOT NULL,              
    product_type VARCHAR(50),             
    product_name VARCHAR(255),            
    size VARCHAR(10),                     
    colour VARCHAR(50),                   
    price DECIMAL(10, 2),                 
    quantity INT,                         
    description TEXT,                     
    PRIMARY KEY (product_id)              
);

-- Drop and create 'orders' table to store order details
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id INT NOT NULL,                
    customer_id INT NOT NULL,             
    payment VARCHAR(50),                 
    order_date DATE,                      
    delivery_date DATE,                  
    PRIMARY KEY (order_id)                
);

-- Drop and create 'customers' table to store customer information
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id INT NOT NULL,             
    customer_name VARCHAR(255),           
    gender VARCHAR(50),                   
    age INT,                              
    home_address VARCHAR(255),            
    zip_code INT,                         
    city VARCHAR(100),                    
    state VARCHAR(100),                   
    country VARCHAR(100),                 
    PRIMARY KEY (customer_id)             
);

-- ---------------------------------------------------
-- Step 2: Data Loading Settings
-- ---------------------------------------------------

-- Configure MySQL to allow data imports
SHOW VARIABLES LIKE "secure_file_priv";   
SHOW VARIABLES LIKE "local_infile";       
SET GLOBAL local_infile = 1;              

-- Uncomment below code to load data from CSV files if local_infile is enabled and 'not NULL' for secure_file_priv
-- Because I got NULL for secure_file_priv, I uploaded data by using Import Wizard.
-- Data sets are pushed to Github. they can be found under 'data_input' file. 

-- LOAD DATA LOCAL INFILE '/Users/isil/Downloads/DE_SQL_TermProject1_Isil/data_input/sales.csv'
-- FIELDS TERMINATED BY ',' 
-- ENCLOSED BY '"' 
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS 
-- (sales_id, order_id, product_id, price_per_unit, quantity, total_price);

-- ---------------------------------------------------
-- Step 3: Data Examination
-- ---------------------------------------------------

-- Check loaded data in core tables to make sure data is loaded and to understand relationship between tables for EER
SELECT * FROM termproject1.customers LIMIT 10;
SELECT * FROM termproject1.orders LIMIT 10;
SELECT COUNT(DISTINCT order_id) FROM termproject1.orders;		
SELECT COUNT(DISTINCT customer_id) FROM termproject1.orders;	
SELECT * FROM termproject1.products LIMIT 10;
SELECT * FROM termproject1.sales LIMIT 10;
SELECT COUNT(DISTINCT sales_id) FROM termproject1.sales;		
SELECT COUNT(DISTINCT order_id) FROM termproject1.sales;		
SELECT COUNT(DISTINCT product_id) FROM termproject1.sales;		

-- Basic analysis of customer data for insights
SELECT COUNT(DISTINCT state) FROM termproject1.customers;  -- Count of unique states
SELECT DISTINCT gender FROM termproject1.customers;        -- Distinct genders in customer data
SELECT MIN(age), MAX(age) FROM termproject1.customers;     -- Age range

-- ---------------------------------------------------
-- Step 4: Creating Stored Procedure for Data Mart Table
-- ---------------------------------------------------
-- Analytical Data Layer:
-- - The `ShoppingCartInsights` table is a denormalized, consolidated view that combines data from 
--   `sales`, `orders`, `customers`, and `products` tables. It includes derived metrics such as delivery 
--   time and age categories, allowing for efficient querying and analysis.
-- - The stored procedure `CreateShoppingCartInsights` creates this table by transforming and joining 
--   relevant columns from each source table.

DROP PROCEDURE IF EXISTS CreateShoppingCartInsights;

DELIMITER //

CREATE PROCEDURE CreateShoppingCartInsights()
BEGIN
    DROP TABLE IF EXISTS ShoppingCartInsights;

    CREATE TABLE ShoppingCartInsights AS
    SELECT 
        s.sales_id, 
        s.order_id, 
        s.product_id, 
        s.price_per_unit, 
        s.quantity, 
        s.total_price, 
        o.customer_id,
        o.order_date,
        DATEDIFF(o.delivery_date, o.order_date) AS delivery_time, -- Calculates delivery time in days
        c.gender,
        c.age,
        CASE 
            WHEN c.age >= 20 AND c.age < 35 THEN '20-34'
            WHEN c.age >= 35 AND c.age < 50 THEN '35-49'
            WHEN c.age >= 50 AND c.age < 65 THEN '50-64'
            ELSE '65-80'
        END AS age_category,  -- Categorizes customers by age group
        c.state,
        p.product_type,
        p.size,
        p.colour
    FROM termproject1.sales AS s
    LEFT JOIN termproject1.orders AS o ON s.order_id = o.order_id
    LEFT JOIN termproject1.customers AS c ON o.customer_id = c.customer_id
    LEFT JOIN termproject1.products AS p ON s.product_id = p.product_id;
END //
DELIMITER ;

-- Run the stored procedure to create ShoppingCartInsights
CALL CreateShoppingCartInsights();
SELECT * FROM ShoppingCartInsights; -- check whether the table is created and data is stored in it

-- ---------------------------------------------------
-- Step 5: Creating Analytical Views (Data Marts)
-- ---------------------------------------------------
-- Data marts are created as views to support specific analytics.
-- These data marts ensure quick access to frequently used analytics while avoiding repeated complex queries, 
-- and they can be expanded as further insights are needed. 

-- Provide revenue breakdown by age and gender, supporting marketing insights.
-- Age-based revenue breakdown (for marketing insights)
DROP VIEW IF EXISTS RevenueByAgeCategory;
CREATE VIEW RevenueByAgeCategory AS
SELECT 
    age_category,
    SUM(total_price) AS total_revenue,
    ROUND(SUM(total_price) / (SELECT SUM(total_price) FROM ShoppingCartInsights) * 100, 1) AS revenue_percentage
FROM ShoppingCartInsights
GROUP BY age_category
ORDER BY age_category ASC;

SELECT * FROM RevenueByAgeCategory;

-- Gender-based revenue breakdown
DROP VIEW IF EXISTS RevenueByGender;
CREATE VIEW RevenueByGender AS
SELECT 
    gender,
    SUM(total_price) AS total_revenue,
    ROUND(SUM(total_price) / (SELECT SUM(total_price) FROM ShoppingCartInsights) * 100, 1) AS revenue_percentage
FROM ShoppingCartInsights
GROUP BY gender
ORDER BY revenue_percentage DESC;

SELECT * FROM RevenueByGender;

-- Summarizes average delivery times per state to identify areas for logistics improvement.
-- State-based average delivery time analysis (logistics insights)
DROP VIEW IF EXISTS AvgDeliveryTimeByState;
CREATE VIEW AvgDeliveryTimeByState AS
SELECT 
    state,
    ROUND(AVG(delivery_time), 1) AS avg_delivery_time
FROM (
    SELECT DISTINCT order_id, state, delivery_time
    FROM ShoppingCartInsights
) AS unique_orders
GROUP BY state
ORDER BY avg_delivery_time ASC;

SELECT * FROM AvgDeliveryTimeByState;

-- A moving average calculation over a 3-day period for each product, helping to spot significant spikes in demand.
-- Product sales data mart with a 3-day moving average and sales jump flag
DROP VIEW IF EXISTS ProductQuantityMovingAvg;
CREATE VIEW ProductQuantityMovingAvg AS
SELECT 
    product_id,
    order_date,
    SUM(quantity) AS daily_quantity,
    ROUND(AVG(SUM(quantity)) OVER (
        PARTITION BY product_id 
        ORDER BY order_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 1) AS moving_avg_3_days,
    CASE 
        WHEN SUM(quantity) > 1.5 * (AVG(SUM(quantity)) OVER (	-- 1.5 is chosen arbitrarily
            PARTITION BY product_id 
            ORDER BY order_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW))
        THEN 1
        ELSE 0
    END AS is_sales_jump
FROM ShoppingCartInsights
GROUP BY product_id, order_date
ORDER BY product_id, order_date;

SELECT * FROM ProductQuantityMovingAvg;

-- Highlight revenue by product type and top 10 revenue-generating products, aiding in product performance analysis.
-- Product revenue breakdown
DROP VIEW IF EXISTS RevenueByProductType;
CREATE VIEW RevenueByProductType AS
SELECT 
    product_type,
    SUM(total_price) AS total_revenue,
    ROUND(SUM(total_price) / (SELECT SUM(total_price) FROM ShoppingCartInsights) * 100, 1) AS revenue_percentage
FROM ShoppingCartInsights
GROUP BY product_type
ORDER BY revenue_percentage DESC;

SELECT * FROM RevenueByProductType;

-- Top 10 revenue-generating products
DROP VIEW IF EXISTS Top10RevenueProducts;
CREATE VIEW Top10RevenueProducts AS
SELECT
    ROW_NUMBER() OVER (ORDER BY SUM(total_price) DESC) AS revenue_rank,
    product_id,
    SUM(total_price) AS total_revenue
FROM ShoppingCartInsights
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10;

SELECT * FROM Top10RevenueProducts;

-- Tracks average revenue per month, useful for identifying seasonality and revenue trends.
-- Monthly average revenue
DROP VIEW IF EXISTS MonthlyAverageRevenue;
CREATE VIEW MonthlyAverageRevenue AS
SELECT 
    DATE_FORMAT(order_date, '%Y-%m') AS yearmonth,
    ROUND(AVG(total_price),1) AS avg_monthly_revenue
FROM ShoppingCartInsights
GROUP BY yearmonth
ORDER BY yearmonth;

SELECT * FROM MonthlyAverageRevenue;
-- ---------------------------------------------------
-- Step 6: ETL Trigger on Customer Updates
-- ---------------------------------------------------
-- ETL Process:
-- - An ETL trigger (`after_customer_update`) is created to refresh `ShoppingCartInsights` whenever there 
--   is an update to the `customers` table. This ensures that any changes to customer information are 
--   immediately reflected in the analytical data layer.
-- - Additional triggers or scheduled events could be added if further automation or incremental updates 
--   are required for other tables.


-- Trigger to update ShoppingCartInsights when customer details are modified
DROP TRIGGER IF EXISTS after_customer_update;

DELIMITER $$

CREATE TRIGGER after_customer_update
AFTER UPDATE ON customers
FOR EACH ROW
BEGIN
    -- Update the ShoppingCartInsights table with the new customer data
    UPDATE ShoppingCartInsights
    SET 
        gender = NEW.gender,
        age = NEW.age,
        state = NEW.state,
        age_category = CASE 
            WHEN NEW.age >= 20 AND NEW.age < 35 THEN '20-34'
            WHEN NEW.age >= 35 AND NEW.age < 50 THEN '35-49'
            WHEN NEW.age >= 50 AND NEW.age < 65 THEN '50-64'
            ELSE '65-80'
        END
    WHERE customer_id = NEW.customer_id;
END $$

DELIMITER ;

-- ---------------------------------------------------
-- Step 7:Testing the Trigger
-- ---------------------------------------------------

SET SQL_SAFE_UPDATES = 0; 	-- Step 1: Disable Safe Update Mode

-- Let's check what is the data recorded for the customer before the update
SELECT * FROM ShoppingCartInsights
WHERE customer_id = 64; 

-- Step 2: Update Customer Record (customer_id = 64)
-- Simulate an update to the customer's information:
-- - Assume the customer's age was previously incorrect and is now corrected to 47.
-- - Update address details for the customer
UPDATE customers
SET 
    age = 47,                                     
    home_address = '123 New Address Suite 200',   
    city = 'New Cityville',                       
    state = 'New State',                         
    zip_code = 9999                               
WHERE customer_id = 64;

-- Step 3: Verify the Trigger Update in ShoppingCartInsights
SELECT * FROM ShoppingCartInsights
WHERE customer_id = 64;
