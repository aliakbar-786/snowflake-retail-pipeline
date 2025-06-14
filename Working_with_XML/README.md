# 📘 Working with XML Data in Snowflake | Handle Semi-Structured Data

This project demonstrates how to work with semi-structured **XML** data using **Snowflake** cloud data warehouse. It covers the full workflow from setting up S3 integration, defining file formats, loading XML into staging tables, and transforming it into structured tables.

---

## 📂 Project Structure

```bash
├── dummydata.xml                 # Sample XML file with book records
├── xml_project.sql              # Fully commented Snowflake SQL script
└── README.md                    # This documentation file

🧠 Key Concepts Covered
Snowflake Storage Integration with AWS S3

Creating XML File Format object

Loading data via external stages

Using VARIANT, FLATTEN, and XMLGET() to parse XML

Inserting clean data into structured target tables

Running analytics on parsed XML content

🔧 Prerequisites
Snowflake account with:

A working storage integration for AWS S3

Access to create databases, schemas, stages, and tables

🛠️ How to Run
Upload the dummydata.xml file to your S3 bucket path like:


s3://buckt-for-semi-str/xml/dummydata.xml
Modify and execute the SQL script:

Replace bucket names and integration names as per your environment.

Run xml_project.sql in Snowflake Worksheet or Snowsight.

Explore and validate:

Raw XML preview using SELECT * FROM @stage/...

Flatten and extract fields from XML into structured rows

Run sample aggregation query on authors

🚀 Output Example
After executing the queries, you’ll get a clean table like:

book_id	author	title	genre	price	publish_date	description
bk101	Gambardella, Matthew	XML Developer's Guide	Computer	44.95	2000-10-01	An in-depth look at XML apps.
bk102	Ralls, Kim	Midnight Rain	Fantasy	5.95	2000-12-16	A former architect battles zombies.

below are query 

// Alter storage integration to add xml files location
alter storage integration aws_s3_integration
set STORAGE_ALLOWED_LOCATIONS = ('s3://buckt-for-semi-str/xml/','s3://buckt-for-semi-str/json/');

DESC integration aws_s3_integration

// Create required datbase and schemas
create database if not exists my_db
create schema if not exists my_db.file_formats;
create schema if not exists my_db.external_stages;
create schema if not exists my_db.stage_tbls;
create schema if not exists my_db.intg_tbls;

// Create file format object for xml files
CREATE OR REPLACE file format my_db.file_formats.xml_fileformat
    type = xml;

// Create stage on external s3 location
CREATE OR REPLACE STAGE my_db.external_stages.aws_s3_xml
    URL = 's3://buckt-for-semi-str/xml/'
    STORAGE_INTEGRATION = aws_s3_integration
    FILE_FORMAT = my_db.file_formats.xml_fileformat ;

// Listing files under your s3 xml bucket
list @my_db.external_stages.aws_s3_xml;

// View data from xml file
select * from @my_db.external_stages.aws_s3_xml/dummydata.xml;

// Create variant table to load xml file
CREATE OR REPLACE TABLE my_db.stage_tbls.STG_BOOKS(xml_data variant);

// Load xml file to variant table
copy into my_db.stage_tbls.STG_BOOKS
from @my_db.external_stages.aws_s3_xml
files=('dummydata.xml')
force=TRUE;

// Query stage table
select * from my_db.stage_tbls.STG_BOOKS;

// Create Target table to load final data 
CREATE OR REPLACE TABLE intg_tbls.BOOKS
( 
 book_id varchar(20) not null,
 author varchar(50),
 title varchar(50),
 genre varchar(20),
 price number(10,2),
 publish_date date,
 description varchar(255),
 PRIMARY KEY(book_id)
);


// To get the root element name
select xml_data:"@" from my_db.stage_tbls.STG_BOOKS;

// To get root element value
select xml_data:"$" from my_db.stage_tbls.STG_BOOKS;

select xmlget (xml_data, 'book') from my_db.stage_tbls.STG_BOOKS;

// Get the content using xmlget function, index position
select xmlget(xml_data,'book',0):"$" from my_db.stage_tbls.STG_BOOKS;
select xmlget(xml_data,'book',1):"$" from my_db.stage_tbls.STG_BOOKS;

// Fetch actual data from file
SELECT 
XMLGET(bk.value, 'id' ):"$" as "book_id",
XMLGET(bk.value, 'author' ):"$" as "author"
FROM my_db.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(to_array(STG_BOOKS.xml_data:"$" )) bk;

// Fetch data and assign datatypes
SELECT 
XMLGET(bk.value, 'id' ):"$" :: varchar as "book_id",
XMLGET(bk.value, 'author' ):"$" :: varchar as "author",
XMLGET(bk.value, 'title' ):"$" :: varchar as "title",
XMLGET(bk.value, 'genre' ):"$" :: varchar as "genre",
XMLGET(bk.value, 'price' ):"$" :: number(10,2) as "price",
XMLGET(bk.value, 'publish_date' ):"$" :: date as "publish_date",
XMLGET(bk.value, 'description' ):"$" :: varchar as "description"

FROM my_db.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(to_array(STG_BOOKS.xml_data:"$" )) bk;


// Insert data from stage table to final target table
INSERT INTO intg_tbls.BOOKS
SELECT 
XMLGET(bk.value, 'id' ):"$" :: varchar as "book_id",
XMLGET(bk.value, 'author' ):"$" :: varchar as "author",
XMLGET(bk.value, 'title' ):"$" :: varchar as "title",
XMLGET(bk.value, 'genre' ):"$" :: varchar as "genre",
XMLGET(bk.value, 'price' ):"$" :: number(10,2) as "price",
XMLGET(bk.value, 'publish_date' ):"$" :: date as "publish_date",
XMLGET(bk.value, 'description' ):"$" :: varchar as "description"
FROM my_db.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(to_array(STG_BOOKS.xml_data:"$" )) bk;


// View final data
SELECT * FROM intg_tbls.BOOKS;

-----Good SQL-------
select
xmlget (value, 'author'):"$"::string author 
,count(*)
from my_db.stage_tbls.STG_BOOKS,
lateral flatten(input => xml_data:"$") list
group by 1
order by 2 desc;

---get the third element in the array---
select
 xml_data:"$"[2]
from my_db.stage_tbls.STG_BOOKS;

-----get the element values of the third element in the array
select
 xml_data:"$"[2]              ---the 3rd record
,xml_data:"$"[2]:"$"[0]       ---the first element of the 3rd record
,xml_data:"$"[2]:"$"[0]:"@"   ---the name of the first element of the
,xml_data:"$"[2]:"$"[0]:"$"   ---the value of the first element of the
 from my_db.stage_tbls.STG_BOOKS;

