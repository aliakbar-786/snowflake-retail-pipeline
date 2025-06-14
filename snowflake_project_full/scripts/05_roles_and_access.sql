
-- Create roles and assign privileges
CREATE OR REPLACE ROLE analyst_role;
GRANT USAGE ON DATABASE retail_db TO ROLE analyst_role;
GRANT USAGE ON SCHEMA sales_data TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA sales_data TO ROLE analyst_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA sales_data TO ROLE analyst_role;

-- (Optional) assign to a user
-- GRANT ROLE analyst_role TO USER your_user_name;
