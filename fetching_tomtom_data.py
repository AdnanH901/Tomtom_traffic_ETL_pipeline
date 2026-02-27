from airflow.operators.python import PythonOperator, BranchPythonOperator # type: ignore
from airflow.operators.empty import EmptyOperator # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator # type: ignore
from airflow import DAG # type: ignore
from datetime import datetime, date
import pandas as pd
import requests
import json
import csv
import os

file_path = "/opt/airflow/dags/data/tomtom_data.csv"
API_KEY = "2hwlZA6iPIKOZz6l53YeHmci6FeMFxqV"  # TomTom API Key
lat, lon = 51.5177, 0.1948  # Coordinates for London, Rainham

def traffic_data_fetch(lat, lon, ti):
    '''
    Fetch traffic data from TomTom API for given latitude and longitude.
    '''
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={API_KEY}&point={lat},{lon}"
    response = requests.get(url)
    data = response.json()
    tomtom_data = [{
        "frc": data["flowSegmentData"]["frc"],
        "currentSpeed": data["flowSegmentData"]["currentSpeed"],
        "freeFlowSpeed": data["flowSegmentData"]["freeFlowSpeed"],
        "currentTravelTime": data["flowSegmentData"]["currentTravelTime"],
        "freeFlowTravelTime": data["flowSegmentData"]["freeFlowTravelTime"],
        "confidence":   data["flowSegmentData"]["confidence"],
        "roadClosure": data["flowSegmentData"]["roadClosure"],
        "coordinates": json.dumps(data["flowSegmentData"]["coordinates"]["coordinate"])
    }]
    ti.xcom_push(key="requested_data", value=tomtom_data)

def create_initial_csv(ti):
    '''
    Create initial CSV file with TomTom data. 
    This works in combination with the data_existence_check task. 
    If the data_existence_check function detects that the CSV file 
    DOES NOT exist, it will call out this function to create it.
    '''
    initial_data = ti.xcom_pull(key="requested_data", task_ids="fetch_tomtom_data")[0]
    with open(file_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=initial_data.keys())
        writer.writeheader()
        writer.writerow(initial_data)

def data_merge(ti):
    '''
    Append new TomTom data into the existing CSV file.
    This works in combination with the data_existence_check task. 
    If the data_existence_check function detects that the CSV file 
    DOES exist, it will call out this function to append to it.
    '''
    new_row = ti.xcom_pull(key="requested_data", task_ids="fetch_tomtom_data")[0]
    with open(file_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=new_row.keys())
        writer.writerow(new_row)

def data_transform():
    '''
    General transformation function that is in charge of the transform part of ETL process.
    It calculates delay and congestion level and adds them as new columns to the CSV file.
    1. Delay is calculated as the difference between currentTravelTime and freeFlowTravelTime
    2. Congestion level is calculated as 1 - (currentSpeed / freeFlowSpeed)
    3. Time based slowdowns is calculated as (currentTravelTime / freeFlowTravelTime) - 1
    '''
    df = pd.read_csv(file_path)
    df["delay"] = df["currentTravelTime"] - df["freeFlowTravelTime"]
    df["congestion_level"] = 1 - df["currentSpeed"] / df["freeFlowSpeed"]
    df["time_based_slowdowns"] = df["currentTravelTime"] / df["freeFlowTravelTime"] - 1
    df.to_csv(file_path, index=False)

def data_check():
    '''
    Check if the CSV file already exists.
    If it exists, return "row_append" to append new data.
    If it doesn't exist, return "initialise_csv" to create the initial CSV file.
    '''
    return "row_append" if os.path.exists(file_path) else "initialise_csv"

with DAG(
    dag_id="tomtom_traffic_data_dag",
    start_date=datetime(2025, 11, 23),
    schedule="* * * * *",
    catchup=False,
) as dag:
    # This is in charge of all data fetching from TomTom API.
    # Use ti.xcom_pull(key="requested_data", task_ids="fetch_tomtom_data")[0] to get the data.
    # NOTE: USE THIS ^^^ TO GET THE DATA FROM THE TOMTOM API.
    fetch_tomtom_data = PythonOperator(
        task_id="fetch_tomtom_data",
        python_callable=traffic_data_fetch,
        op_kwargs={"lat": lat, "lon": lon},
    )

    # This task checks if the CSV file already exists or not.
    # If it doesn't exist, it creates one using the create_initial_csv task.
    # If it does exist, it appends new data into it using the data_merge task.
    data_existence_check = BranchPythonOperator(
        task_id="data_existence_check",
        python_callable=data_check,
    )

    # This task creates the initial CSV file with TomTom data if there is no existing CSV file.
    initialise_csv = PythonOperator(
        task_id="initialise_csv",
        python_callable=create_initial_csv,
    )

    # This task appends new data into the existing CSV file.
    row_append = PythonOperator(
        task_id="row_append",
        python_callable=data_merge,
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=data_transform,
        trigger_rule="none_failed",
    )

    initialise_postgres = SQLExecuteQueryOperator(
        task_id="intialise_postgres",
        conn_id="tomtom_data_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS tomtom_data (
            frc TEXT,
            confidence FLOAT,
            coordinates TEXT,
            roadClosure BOOLEAN,
            currentSpeed FLOAT,
            freeFlowSpeed FLOAT,
            currentTravelTime FLOAT,
            freeFlowTravelTime FLOAT,
            delay FLOAT,
            congestion_level FLOAT,
            time_based_slowdowns FLOAT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """,
    )

    row_append_postgres = SQLExecuteQueryOperator(
        task_id="row_append_postgres",
        conn_id="tomtom_data_conn",
        sql="""
        INSERT INTO tomtom_data (
            frc,
            confidence,
            coordinates,
            roadClosure,
            currentSpeed,
            freeFlowSpeed,
            currentTravelTime,
            freeFlowTravelTime,
            delay,
            congestion_level,
            time_based_slowdowns
        )
        VALUES (
            '{{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["frc"] }}',
            {{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["confidence"] }},
            '{{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["coordinates"] }}',
            {{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["roadClosure"] }},
            {{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["currentSpeed"] }},
            {{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["freeFlowSpeed"] }},
            {{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["currentTravelTime"] }},
            {{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["freeFlowTravelTime"] }},
            {{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["currentTravelTime"] - ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["freeFlowTravelTime"] }},
            1 - ({{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["currentSpeed"] }}/{{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["freeFlowSpeed"] }}),
            ({{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["currentTravelTime"] }}/{{ ti.xcom_pull(task_ids="fetch_tomtom_data", key="requested_data")[0]["freeFlowTravelTime"] }}) - 1
        );
        """,
    )
    
    # Scrap operator used as a placeholder for any upcoming operations.
    # NOTE: DELETE THIS AFTER DAG IS FULLY BUILT.
    dummy_task = EmptyOperator(
        task_id="dummy_task",
    )

    # Task order:
    fetch_tomtom_data >> data_existence_check >> [row_append, initialise_csv] >> transform_data

    fetch_tomtom_data >> initialise_postgres >> row_append_postgres