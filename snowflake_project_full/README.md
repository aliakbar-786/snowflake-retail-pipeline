
# Snowflake Retail Data Warehouse Demo

This project demonstrates an end-to-end data warehouse setup in Snowflake using internal staging, COPY INTO, transformation SQL, and role-based access.

## 🧱 Structure
- Load CSVs into internal stage
- Use COPY INTO to load raw tables
- Apply transformations using SQL
- Setup role-based access control (RBAC)

## 📁 Files
- `datasets/`: Sample CSV files
- `scripts/`: SQL scripts for setup and loading

## 🛠️ Requirements
- Snowflake account
- Web UI or SnowSQL

## 🚀 Steps
1. Run `01_create_db_schema.sql`
2. Run `02_create_stage_and_format.sql`
3. Upload CSV files to `@my_internal_stage` using Web UI
4. Run `03_copy_into_raw.sql`
5. Run `04_transformations.sql`
6. Run `05_roles_and_access.sql` for access control

