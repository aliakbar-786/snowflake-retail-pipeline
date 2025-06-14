-- Step 1: Allow Snowflake access to XML/JSON folders on S3
ALTER STORAGE INTEGRATION aws_s3_integration
SET STORAGE_ALLOWED_LOCATIONS = ('s3://buckt-for-semi-str/xml/', 's3://buckt-for-semi-str/json/');

-- Step 2: View current integration configuration
DESC INTEGRATION aws_s3_integration;

-- Step 3: Create database and schemas if not already present
CREATE DATABASE IF NOT EXISTS my_db;
CREATE SCHEMA IF NOT EXISTS my_db.file_formats;
CREATE SCHEMA IF NOT EXISTS my_db.external_stages;
CREATE SCHEMA IF NOT EXISTS my_db.stage_tbls;
CREATE SCHEMA IF NOT EXISTS my_db.intg_tbls;

-- Step 4: Define XML file format
CREATE OR REPLACE FILE FORMAT my_db.file_formats.xml_fileformat
    TYPE = XML;

-- Step 5: Create stage pointing to your XML S3 bucket
CREATE OR REPLACE STAGE my_db.external_stages.aws_s3_xml
    URL = 's3://buckt-for-semi-str/xml/'
    STORAGE_INTEGRATION = aws_s3_integration
    FILE_FORMAT = my_db.file_formats.xml_fileformat;

-- Step 6: List files in the S3 location
LIST @my_db.external_stages.aws_s3_xml;

-- Step 7: Preview XML file directly from stage
SELECT * FROM @my_db.external_stages.aws_s3_xml/dummydata.xml;

-- Step 8: Create staging table to hold XML as VARIANT type
CREATE OR REPLACE TABLE my_db.stage_tbls.STG_BOOKS (
    xml_data VARIANT
);

-- Step 9: Load XML file into staging table
COPY INTO my_db.stage_tbls.STG_BOOKS
FROM @my_db.external_stages.aws_s3_xml
FILES = ('dummydata.xml')
FORCE = TRUE;

-- Step 10: Review loaded data
SELECT * FROM my_db.stage_tbls.STG_BOOKS;

-- Step 11: Create final structured table
CREATE OR REPLACE TABLE my_db.intg_tbls.BOOKS (
    book_id VARCHAR(20) NOT NULL,
    author VARCHAR(50),
    title VARCHAR(50),
    genre VARCHAR(20),
    price NUMBER(10,2),
    publish_date DATE,
    description VARCHAR(255),
    PRIMARY KEY (book_id)
);

-- Step 12: Check root attributes and values
SELECT xml_data:"@" FROM my_db.stage_tbls.STG_BOOKS;
SELECT xml_data:"$" FROM my_db.stage_tbls.STG_BOOKS;

-- Step 13: Explore book nodes
SELECT XMLGET(xml_data, 'book') FROM my_db.stage_tbls.STG_BOOKS;
SELECT XMLGET(xml_data, 'book', 0):"$" FROM my_db.stage_tbls.STG_BOOKS;
SELECT XMLGET(xml_data, 'book', 1):"$" FROM my_db.stage_tbls.STG_BOOKS;

-- Step 14: Flatten and extract XML fields
SELECT 
    XMLGET(bk.value, 'id'):"$" AS book_id,
    XMLGET(bk.value, 'author'):"$" AS author
FROM my_db.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(TO_ARRAY(xml_data:"$")) bk;

-- Step 15: Fully extract and cast fields
SELECT 
    XMLGET(bk.value, 'id'):"$"::VARCHAR AS book_id,
    XMLGET(bk.value, 'author'):"$"::VARCHAR AS author,
    XMLGET(bk.value, 'title'):"$"::VARCHAR AS title,
    XMLGET(bk.value, 'genre'):"$"::VARCHAR AS genre,
    XMLGET(bk.value, 'price'):"$"::NUMBER(10,2) AS price,
    XMLGET(bk.value, 'publish_date'):"$"::DATE AS publish_date,
    XMLGET(bk.value, 'description'):"$"::VARCHAR AS description
FROM my_db.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(TO_ARRAY(xml_data:"$")) bk;

-- Step 16: Insert final data into target table
INSERT INTO my_db.intg_tbls.BOOKS
SELECT 
    XMLGET(bk.value, 'id'):"$"::VARCHAR AS book_id,
    XMLGET(bk.value, 'author'):"$"::VARCHAR AS author,
    XMLGET(bk.value, 'title'):"$"::VARCHAR AS title,
    XMLGET(bk.value, 'genre'):"$"::VARCHAR AS genre,
    XMLGET(bk.value, 'price'):"$"::NUMBER(10,2) AS price,
    XMLGET(bk.value, 'publish_date'):"$"::DATE AS publish_date,
    XMLGET(bk.value, 'description'):"$"::VARCHAR AS description
FROM my_db.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(TO_ARRAY(xml_data:"$")) bk;

-- Step 17: Final result check
SELECT * FROM my_db.intg_tbls.BOOKS;

-- Bonus: Author frequency check
SELECT 
    XMLGET(value, 'author'):"$"::STRING AS author,
    COUNT(*) AS total_books
FROM my_db.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(input => xml_data:"$") list
GROUP BY 1
ORDER BY 2 DESC;

-- Bonus: Explore structure of 3rd array element
SELECT
    xml_data:"$"[2],
    xml_data:"$"[2]:"$"[0],
    xml_data:"$"[2]:"$"[0]:"@",
    xml_data:"$"[2]:"$"[0]:"$"
FROM my_db.stage_tbls.STG_BOOKS;
