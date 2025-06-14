-- Working with JSON Data in Snowflake | Handle Semi-Structured Data

-- Create and use a database
CREATE OR REPLACE DATABASE MYOWN_DB;
USE DATABASE MYOWN_DB;

-- Create required schemas
CREATE OR REPLACE SCHEMA MYOWN_DB.external_stages;
CREATE OR REPLACE SCHEMA MYOWN_DB.STAGE_TBLS;
CREATE OR REPLACE SCHEMA MYOWN_DB.INTG_TBLS;

-- Create a schema for file formats
CREATE SCHEMA MYOWN_DB.FILE_FORMATS;

-- Create file format for JSON files
CREATE OR REPLACE FILE FORMAT MYOWN_DB.file_formats.FILE_FORMAT_JSON TYPE = JSON;

-- Optional: show all schemas in the database
SHOW SCHEMAS IN MYOWN_DB;

-- Create a storage integration for AWS S3
CREATE STORAGE INTEGRATION aws_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::954976319757:role/aws-roll-semi-str'
STORAGE_ALLOWED_LOCATIONS = ('s3://buckt-for-semi-str/json/');

-- Create an external stage for accessing JSON files in S3
CREATE OR REPLACE STAGE MYOWN_DB.external_stages.STAGE_JSON
STORAGE_INTEGRATION = aws_s3_integration
URL = 's3://buckt-for-semi-str/json/';

-- (Optional Azure version commented out)
-- CREATE OR REPLACE STAGE MYOWN_DB.external_stages.STAGE_JSON
-- STORAGE_INTEGRATION = s3_int
-- URL = 's3://awss3bucketjana/json/';

-- Describe the storage integration
DESC INTEGRATION aws_s3_integration;

-- List the files present in the stage
LIST @MYOWN_DB.external_stages.STAGE_JSON;

-- Create a staging table to load raw JSON data
CREATE OR REPLACE TABLE MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW(raw_file variant);

-- Load JSON file into the staging table
COPY INTO MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW 
FROM @MYOWN_DB.external_stages.STAGE_JSON
FILE_FORMAT = MYOWN_DB.file_formats.FILE_FORMAT_JSON
FILES = ('dummy data.json');

-- View raw JSON data
SELECT * FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

-- Extract a single JSON attribute
SELECT raw_file:Name::string as Name FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

-- Extract an array element
SELECT raw_file:Name::string as Name, raw_file:Pets[0]::string as Pet 
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

-- Get size of the array
SELECT raw_file:Name::string as Name, ARRAY_SIZE(RAW_FILE:Pets) as PETS_AR_SIZE 
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

-- Get max size among all records
SELECT max(ARRAY_SIZE(RAW_FILE:Pets)) as PETS_AR_SIZE 
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

-- Extract nested fields from JSON
SELECT raw_file:Name::string as Name,
       raw_file:Address."House Number"::string as House_No,
       raw_file:Address.City::string as City,
       raw_file:Address.State::string as State
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

-- Parse full records including nested fields and multiple pets
SELECT raw_file:Name::string as Name,
       raw_file:Gender::string as Gender,
       raw_file:DOB::date as DOB,
       raw_file:Pets[0]::string as Pets,
       raw_file:Address."House Number"::string as House_No,
       raw_file:Address.City::string as City,
       raw_file:Address.State::string as State,
       raw_file:Phone.Work::number as Work_Phone,
       raw_file:Phone.Mobile::number as Mobile_Phone
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
UNION ALL
SELECT raw_file:Name::string as Name,
       raw_file:Gender::string as Gender,
       raw_file:DOB::date as DOB,
       raw_file:Pets[1]::string as Pets,
       raw_file:Address."House Number"::string as House_No,
       raw_file:Address.City::string as City,
       raw_file:Address.State::string as State,
       raw_file:Phone.Work::number as Work_Phone,
       raw_file:Phone.Mobile::number as Mobile_Phone
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
UNION ALL
SELECT raw_file:Name::string as Name,
       raw_file:Gender::string as Gender,
       raw_file:DOB::date as DOB,
       raw_file:Pets[2]::string as Pets,
       raw_file:Address."House Number"::string as House_No,
       raw_file:Address.City::string as City,
       raw_file:Address.State::string as State,
       raw_file:Phone.Work::number as Work_Phone,
       raw_file:Phone.Mobile::number as Mobile_Phone
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
WHERE Pets is not null;

-- Load parsed data into final table
CREATE TABLE MYOWN_DB.INTG_TBLS.PETS_DATA AS
SELECT raw_file:Name::string as Name,
       raw_file:Gender::string as Gender,
       raw_file:DOB::date as DOB,
       raw_file:Pets[0]::string as Pets,
       raw_file:Address."House Number"::string as House_No,
       raw_file:Address.City::string as City,
       raw_file:Address.State::string as State,
       raw_file:Phone.Work::number as Work_Phone,
       raw_file:Phone.Mobile::number as Mobile_Phone
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
UNION ALL
SELECT raw_file:Name::string as Name,
       raw_file:Gender::string as Gender,
       raw_file:DOB::date as DOB,
       raw_file:Pets[1]::string as Pets,
       raw_file:Address."House Number"::string as House_No,
       raw_file:Address.City::string as City,
       raw_file:Address.State::string as State,
       raw_file:Phone.Work::number as Work_Phone,
       raw_file:Phone.Mobile::number as Mobile_Phone
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
UNION ALL
SELECT raw_file:Name::string as Name,
       raw_file:Gender::string as Gender,
       raw_file:DOB::date as DOB,
       raw_file:Pets[2]::string as Pets,
       raw_file:Address."House Number"::string as House_No,
       raw_file:Address.City::string as City,
       raw_file:Address.State::string as State,
       raw_file:Phone.Work::number as Work_Phone,
       raw_file:Phone.Mobile::number as Mobile_Phone
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
WHERE Pets is not null;

-- View final table
SELECT * FROM MYOWN_DB.INTG_TBLS.PETS_DATA;

-- Truncate final table before reload using FLATTEN
TRUNCATE TABLE MYOWN_DB.INTG_TBLS.PETS_DATA;

-- Reload using FLATTEN for dynamic pet array
INSERT INTO MYOWN_DB.INTG_TBLS.PETS_DATA
SELECT raw_file:Name::string as Name,
       raw_file:Gender::string as Gender,
       raw_file:DOB::date as DOB,
       f1.value::string as Pet,
       raw_file:Address."House Number"::string as House_No,
       raw_file:Address.City::string as City,
       raw_file:Address.State::string as State,
       raw_file:Phone.Work::number as Work_Phone,
       raw_file:Phone.Mobile::number as Mobile_Phone
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW,
TABLE(FLATTEN(raw_file:Pets)) f1;

-- View the final integrated table again
SELECT * FROM MYOWN_DB.INTG_TBLS.PETS_DATA;
