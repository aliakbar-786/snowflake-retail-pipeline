# 📘 Working with JSON Data in Snowflake

This project demonstrates how to handle and parse semi-structured **JSON data** using Snowflake. It includes setting up required schemas, creating file formats, using AWS S3 external stages, flattening array structures, and loading structured data into integration tables.

---

## 📂 Project Structure

snowflake-json-project/
├── scripts/
│ └── json_processing.sql # Fully commented SQL script
├── data/
│ └── dummy data.json # Sample JSON file to be uploaded to S3
└── README.md # This file


---

## 🧱 Features

- ✅ Create database and schema structure
- ✅ Define file format for JSON data
- ✅ Create external stage for AWS S3
- ✅ Load raw JSON data into staging tables
- ✅ Parse nested and array JSON elements
- ✅ Flatten array fields using `FLATTEN()`
- ✅ Store final structured data in integration tables

---

## 📁 Dummy JSON Data Example

Create a file named `dummy data.json` with the following content and upload it to your S3 bucket:

```json
[
  {
    "Name": "Ali",
    "Gender": "Male",
    "DOB": "1990-01-01",
    "Pets": ["Cat", "Dog", "Parrot"],
    "Address": {
      "House Number": "123",
      "City": "Lahore",
      "State": "Punjab"
    },
    "Phone": {
      "Work": 423456789,
      "Mobile": 3001234567
    }
  }
]

🛠️ Prerequisites
Snowflake account with access to:

Warehouses

Storage integration setup (AWS S3 role and policy)

AWS S3 bucket with sample JSON file

Appropriate IAM role with access to the S3 location

🚀 SQL Scripts
All SQL scripts are in scripts/json_processing.sql and include:

Creating DB and schemas

Defining file format

Creating storage integration

Creating external stage

Loading raw data into variant table

Extracting individual and nested fields

Using FLATTEN() for arrays

Creating and inserting data into final table

below are Query

-- Processing semi-structured data (Ex.JSON Data)
--CREATE AND USE DATABSE--

CREATE OR REPLACE DATABASE MYOWN_DB;
USE DATABASE MYOWN_DB;
--Creating required schemas
CREATE OR REPLACE SCHEMA MYOWN_DB.external_stages;
CREATE OR REPLACE SCHEMA MYOWN_DB.STAGE_TBLS;
CREATE OR REPLACE SCHEMA MYOWN_DB.INTG_TBLS;

---create file format----
CREATE SCHEMA MYOWN_DB.FILE_FORMATS;

--Creating file format object
CREATE OR REPLACE FILE FORMAT MYOWN_DB.file_formats.FILE_FORMAT_JSON
 TYPE = JSON;

SHOW SCHEMAS IN MYOWN_DB;

--Creating stage object for aws s3---
CREATE STORAGE INTEGRATION aws_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::954976319757:role/aws-roll-semi-str'
STORAGE_ALLOWED_LOCATIONS = ('s3://buckt-for-semi-str/json/');


CREATE OR REPLACE STAGE MYOWN_DB.external_stages.STAGE_JSON
STORAGE_INTEGRATION = aws_s3_integration
URL = 's3://buckt-for-semi-str/json/';


/*  -- for azure----
CREATE OR REPLACE STAGE MYOWN_DB.external_stages.STAGE_JSON
    STORAGE_INTEGRATION = s3_int
    URL = 's3://awss3bucketjana/json/';
*/


DESC INTEGRATION aws_s3_integration;

--Listing files in the stage
LIST @MYOWN_DB.external_stages.STAGE_JSON;

--Creating Stage Table to store RAW Data 
CREATE OR REPLACE TABLE MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW(raw_file variant);


--Copy the RAW data into a Stage Table
COPY INTO MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW 
    FROM @MYOWN_DB.external_stages.STAGE_JSON
    file_format= MYOWN_DB.file_formats.FILE_FORMAT_JSON
    FILES=('dummy data.json');

--View RAW table data
SELECT * FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

--Extracting single column
SELECT raw_file:Name::string as Name FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

--Extracting Array data
SELECT raw_file:Name::string as Name,
       raw_file:Pets[0]::string as Pet 
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

--Get the size of ARRAY
SELECT raw_file:Name::string as Name, ARRAY_SIZE(RAW_FILE:Pets) as PETS_AR_SIZE 
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

SELECT max(ARRAY_SIZE(RAW_FILE:Pets)) as PETS_AR_SIZE 
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

--Extracting nested data
SELECT raw_file:Name::string as Name,
       raw_file:Address."House Number"::string as House_No,
       raw_file:Address.City::string as City,
       raw_file:Address.State::string as State
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;

--Parsing entire file
SELECT raw_file:Name::string as Name,
       raw_file:Gender::string as Gender,
       raw_file:DOB::date as DOB,
       raw_file:Pets[0]::string as Pets,
       raw_file:Address."House Number"::string as House_No,
    raw_file:Address.City::string as City,
    raw_file:Address.State::string as State,
    raw_file:Phone.Work::number as Work_Phone,
    raw_file:Phone.Mobile::number as Mobile_Phone
from MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
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
from MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
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
from MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
WHERE Pets is not null;

--Creating/Loading parsed data to another table
CREATE TABLE MYOWN_DB.INTG_TBLS.PETS_DATA
AS
SELECT raw_file:Name::string as Name,
       raw_file:Gender::string as Gender,
       raw_file:DOB::date as DOB,
       raw_file:Pets[0]::string as Pets,
       raw_file:Address."House Number"::string as House_No,
    raw_file:Address.City::string as City,
    raw_file:Address.State::string as State,
    raw_file:Phone.Work::number as Work_Phone,
    raw_file:Phone.Mobile::number as Mobile_Phone
from MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
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
from MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
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
from MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW
WHERE Pets is not null;

--Viewing final data
SELECT * from MYOWN_DB.INTG_TBLS.PETS_DATA;

--Truncate and Reload by using flatten

TRUNCATE TABLE MYOWN_DB.INTG_TBLS.PETS_DATA;

INSERT INTO MYOWN_DB.INTG_TBLS.PETS_DATA
select  
       raw_file:Name::string as Name,
       raw_file:Gender::string as Gender,
       raw_file:DOB::date as DOB,
       f1.value::string as Pet,
       raw_file:Address."House Number"::string as House_No,
    raw_file:Address.City::string as City,
    raw_file:Address.State::string as State,
    raw_file:Phone.Work::number as Work_Phone,
    raw_file:Phone.Mobile::number as Mobile_Phone
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW, 
table(flatten(raw_file:Pets)) f1;


--Viewing final data
SELECT * from MYOWN_DB.INTG_TBLS.PETS_DATA;

