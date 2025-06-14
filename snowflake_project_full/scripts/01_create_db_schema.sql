
-- Create database and schema
CREATE OR REPLACE DATABASE retail_db;
USE DATABASE retail_db;

CREATE OR REPLACE SCHEMA sales_data;
USE SCHEMA sales_data;

-- Create raw tables
CREATE OR REPLACE TABLE raw_sales (
    sale_id INT,
    customer_id INT,
    product_id INT,
    sale_date DATE,
    quantity INT,
    unit_price NUMBER(10,2)
);

CREATE OR REPLACE TABLE raw_customers (
    customer_id INT,
    customer_name STRING,
    city STRING
);

CREATE OR REPLACE TABLE raw_products (
    product_id INT,
    product_name STRING,
    category STRING
);
