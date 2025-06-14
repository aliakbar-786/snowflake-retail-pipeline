
-- Load data from stage into raw tables
COPY INTO raw_sales
FROM @my_internal_stage/sales.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

COPY INTO raw_customers
FROM @my_internal_stage/customers.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

COPY INTO raw_products
FROM @my_internal_stage/products.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);
