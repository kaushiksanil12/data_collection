🇮🇳 India Groundwater ETL to Oracle Pipeline
This project provides a robust ETL (Extract, Transform, Load) pipeline to fetch daily groundwater level data from the India-WRIS (Water Resources Information System) portal and load it into an Oracle database.

The script is designed for efficiency and reliability, featuring parallel data extraction, comprehensive data cleaning, and batched database insertions. It can process data for a specified date range, making it suitable for both initial data loading and daily updates.

Key Features ✨
Parallel Data Extraction: Utilizes a thread pool (ThreadPoolExecutor) to fetch data for multiple districts concurrently, significantly speeding up the extraction process.

Automated ETL Process: Implements a full Extract, Transform, and Load workflow.

Dynamic Date Range Processing: Can run the ETL process for a single day or a range of dates, providing flexibility for data backfilling and daily runs.

Data Cleaning and Transformation:

Removes unnecessary columns from the raw API response.

Converts date strings into proper datetime objects.

Handles and removes duplicate records based on the station code and timestamp.

Validates critical data points like latitude, longitude, and dataValue to ensure data quality.

Resilient and Fault-Tolerant:

Includes an automatic retry mechanism for API requests to handle intermittent network issues or server errors.

Efficient Database Loading:

Processes and inserts data into the Oracle database in batches to optimize performance and reduce database load.

Option to automatically delete existing records for a specific date before inserting new data, ensuring data integrity and preventing duplication.

Detailed Statistics and Verification:

Provides a comprehensive summary of the ETL run, including the number of districts processed, records extracted, duplicates removed, and records loaded into Oracle.

Includes a verification step to confirm the total data loaded into the Oracle table.

How It Works ⚙️
The pipeline follows these steps:

Setup Connection: Establishes and verifies the connection to the specified Oracle database.

Date Iteration: Loops through each date within the specified start and end date range.

Delete (Optional): If configured, it first deletes all existing records in the target Oracle table for the current processing date. This prevents data duplication on re-runs.

Extract & Transform:

A list of all districts across India is prepared.

The script dispatches parallel worker threads, one for each district, to fetch data from the India-WRIS API.

For each district's data, it performs a transformation process: removing irrelevant columns, standardizing data types, and dropping duplicates and invalid entries.

Load:

The cleaned records are collected in a buffer.

Once the buffer reaches a predefined batch_size, the data is loaded into the GROUNDWATER_MONITORING table in the Oracle database.

Any remaining records in the buffer are loaded in a final batch.

Statistics & Verification: After processing all dates, the script prints detailed statistics about the ETL process and queries the Oracle database to verify the total number of records successfully inserted.

Requirements 📋
Software
Python 3.8+

An accessible Oracle Database (e.g., Oracle XE).

Python Libraries
You can install the required libraries using pip:

Bash

pip install requests pandas sqlalchemy oracledb tqdm
requests: For making HTTP requests to the India-WRIS API.

pandas: For efficient data manipulation and transformation.

sqlalchemy: For connecting to the Oracle database.

oracledb: The Python driver for Oracle Database.

tqdm: To display a smart progress bar during data collection.

Configuration 📝
Before running the script, you need to configure the following:

1. Oracle Database Connection
Update the oracle_config dictionary with your database credentials:

Python

oracle_config = {
    'username': 'C##GROUNDWATER_DB',
    'password': 'groundwater2025',
    'host': 'localhost',
    'port': '1521',
    'service_name': 'XE'  # Use 'XEPDB1' for pluggable DBs if needed
}
2. ETL Parameters in main()
Adjust the parameters inside the main() function to control the ETL run:

Python

def main():
    # ...
    oracle_etl.run_etl_over_date_range(
        start_date_str='2025-01-01',   # Set your desired start date
        end_date_str='2025-01-03',     # Set your desired end date
        batch_size=1000,               # Adjust batch size for DB insertion
        delete_existing=True           # Set to True to clear old data for the date
    )
    # ...
start_date_str / end_date_str: Define the date range for data collection.

batch_size: The number of records to hold in memory before loading to the database.

delete_existing: Set to True if you want to replace the data for the given dates. Set to False to append data.

3. Oracle Table Schema
Ensure you have a table in your Oracle database with a structure that matches the data being loaded. Here is a sample DDL for the target table:

SQL

CREATE TABLE GROUNDWATER_MONITORING (
    STATION_CODE VARCHAR2(50),
    STATION_NAME VARCHAR2(255),
    STATION_TYPE VARCHAR2(100),
    LATITUDE NUMBER,
    LONGITUDE NUMBER,
    AGENCY_NAME VARCHAR2(100),
    STATE VARCHAR2(100),
    DISTRICT VARCHAR2(100),
    DATA_ACQUISITION_MODE VARCHAR2(100),
    STATION_STATUS VARCHAR2(100),
    TEHSIL VARCHAR2(100),
    DATATYPE_CODE VARCHAR2(50),
    DATA_VALUE NUMBER,
    DATA_TIME TIMESTAMP,
    UNIT VARCHAR2(50),
    WELL_TYPE VARCHAR2(100),
    WELL_DEPTH NUMBER,
    WELL_AQUIFER_TYPE VARCHAR2(100),
    BATCH_ID VARCHAR2(100),
    -- Define a primary key for better performance and integrity
    CONSTRAINT pk_groundwater PRIMARY KEY (STATION_CODE, DATA_TIME)
);
How to Use 🚀
Save the Code: Save the provided code as a Python file (e.g., etl_pipeline.py).

Install Libraries: Open your terminal or command prompt and run pip install -r requirements.txt (if you have one) or install the libraries listed above individually.

Configure the Script: Update the oracle_config and the parameters in the main() function as described in the Configuration section.

Run the Script: Execute the script from your terminal:

Bash

python etl_pipeline.py
Monitor the Output: The script will print its progress, including the connection status, the date being processed, the progress of data extraction, and a final summary of the ETL job.
