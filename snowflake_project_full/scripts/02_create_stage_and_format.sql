
-- Create file format
CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;

-- Create internal stage
CREATE OR REPLACE STAGE my_internal_stage
    FILE_FORMAT = my_csv_format;
