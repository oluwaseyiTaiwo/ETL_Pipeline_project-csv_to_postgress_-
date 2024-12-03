# ETL Pipeline Implementation

This project implements an ETL (Extract, Transform, Load) pipeline to process sales and customer data. The pipeline extracts data from CSV files, transforms it according to defined business rules, and loads the results into a PostgreSQL database.

---

## Features
- **Data Extraction:**
  - Reads multiple CSV files and validates their structure.
  - Handles missing or invalid data gracefully with robust logging.

- **Data Transformation:**
  - Cleans and enriches sales data.
  - Applies business rules to calculate order types and customer tenure.
  - Aggregates sales data for reporting.

- **Data Loading:**
  - Loads processed data into PostgreSQL tables with transaction management.
  - Prevents duplicate data and ensures database integrity.

---
## Approach
- **Extraction:**
  - Reads multiple CSV files (sales_data.csv, customer_data.csv) dynamically.
  - Validates the presence of required columns using configurations from config.json.
  - Logs errors for missing or invalid files and columns.

- **Transformation:**
  - Cleans data by converting dates to a standard format (YYYY-MM-DD) and removing invalid rows.
  - Enriches sales data with customer details and computes additional fields:
  - total_value for each order.
  - order_type (e.g., "High-Value Order").
  - customer_tenure in days since signup.
  - Aggregates sales data into a summary table showing total sales and order counts by product.

- **Loading:**
  - Loads transformed data into PostgreSQL tables (customer_data, sales_data, sales_summary) using batch processing.
  - Ensures data integrity with primary and foreign key constraints.
  - Manages duplicate data by checking existing records before insertion.

## Assumptions

- **File Structure and Format:**
  - The input CSV files (sales_data.csv and customer_data.csv) have a consistent structure, including all required columns as specified in the config.json.
  - Dates in the files (order_date, signup_date) follow a parsable format, such as YYYY-MM-DD or YYYY-MM-DD HH:MM:SS.

- **Data Validity:**
  - Columns like customer_id and order_id are assumed to be unique and numeric.
---
## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/oluwaseyiTaiwo/csv_to_postgress_ETL_Pipeline_project.git
cd csv_to_postgress_ETL_Pipeline_project
```

### 2. Install Python and Dependencies
- **Install Python:**
  - Ensure Python 3.8 or higher is installed on your system. Check the version with:
```bash
python --version
```

- **Set Up Virtual Environment:**
  - Create a virtual environment for the project:
```bash
python -m venv venv
source your_env_name/bin/activate
```

- **Install Dependencies:**
  - Install the required Python packages from requirements.txt:
```bash
pip install -r requirements.txt
```

### 3. Set Up PostgreSQL
- Install PostgreSQL
  - Ensure PostgreSQL is installed on your system. Download PostgreSQL.

- Create Database
  - Log in to PostgreSQL and create a new database:

```sql
CREATE DATABASE Take_Home_ETL_Pipeline;
```
- Create Schema
  - Copy and Paste the provided Data_schema.sql script to set up the database tables using the GUI query tool on pgAdmin4

###  4. Configuration
    - Edit config.json
    - update the config.json file with appropriate file paths and database connection details:

```json
{
    "database": {
        "user": "postgres",
        "password": "your_password",
        "host": "localhost",
        "port": 5432,
        "database": "your_database_name"
    }
}
```

###  5. Run the ETL Pipeline
- Execute the Pipeline
  - Run the ETL process using:
```bash
python load_data.py
```


- **Expected Outputs:**
  - Processed data is loaded into PostgreSQL tables:
    - customer_data
    - sales_data
    - sales_summary

- **Testing:**
    - Use the provided sample files (sales_data.csv, customer_data.csv) to validate the pipeline.
    - Verify the PostgreSQL tables for accurate data loading.