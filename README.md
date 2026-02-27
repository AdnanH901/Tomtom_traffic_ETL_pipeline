# Tomtom_traffic_ETL_pipeline
An Apache Airflow DAG that runs every minute to fetch live traffic flow data from the TomTom API given a specific location,  stores it in a CSV, transforms it with delay and congestion metrics, and loads it into Postgres. Demonstrates a complete ETL pipeline with branching, XCom usage, and automated table initialization.
