# TomTom Traffic ETL Pipeline

A production-style ETL pipeline built with Python's Apache Airflow and containerised in Docker. It fetches real-time traffic data from TomTom's Traffic API, transforms it, stores it locally as a CSV and loads it 9inot a PostgreSQL database. 

## Project Overview
This pipeline consists of a scheduled ETL workflow that:
1. Extracts live traffic flow data from the TomTom API.
2. Stores raw data into the CSV file.
3. Transforms the data to compute traffic metrics such as `delay`, `congestion_level` and `time_based_slowdowns`.
4. Loads the data into PostgreSQL.

The DAG **_runs every minute_** and is built using the best practices from Apache Airflow.

## DAG Structure & Architecture
```
fetch_tomtom_data                      fetch_tomtom_data
    ↓                                      ↓
data_existence_check                   initialise_postgres
    ↓                                      ↓
[row_append | initialise_csv]          row_append_postgres
    ↓
transform_data
```

## Tools & Technologies Used
- **Orchestration:** Apache Airflow
- **API Provider:** TomTom Traffic API
- **Database:** PostgreSQL
- **Data Processing:** Pandas, CSV
- **Language:** Python 3

## What This Project Demonstrates
- Airflow branching (BranchPythonOperator)
- XCom usage
- ETL design patterns
- Idempotent table creation
- Derived metric computation
- Production-style DAG structure
- Database integration
