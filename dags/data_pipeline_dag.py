from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from tasks.load_data import load_csv_to_db
from tasks.clean_duplicates import clean_duplicates
from tasks.update_unique_table import update_unique_table

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    "data_pipeline",
    default_args=default_args,
    description="A pipeline to process CSV data and update unique rows",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 2),
    catchup=False,
) as dag:

    # Task 1: Load data from yesterday's folder
    load_yesterday_data = PythonOperator(
        task_id="load_yesterday_data",
        python_callable=load_csv_to_db,
        op_kwargs={"folder_date": (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")},
    )

    # Task 2: Load data from the day-before-yesterday's folder
    load_day_before_data = PythonOperator(
        task_id="load_day_before_data",
        python_callable=load_csv_to_db,
        op_kwargs={"folder_date": (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")},
    )

    # Task 3: Clean duplicates from the main table
    clean_duplicates_task = PythonOperator(
        task_id="clean_duplicates",
        python_callable=clean_duplicates,
    )

    # Task 4: Update the unique_data_table
    update_unique_table_task = PythonOperator(
        task_id="update_unique_table",
        python_callable=update_unique_table,
    )

    # Define task dependencies
    [load_yesterday_data, load_day_before_data] >> clean_duplicates_task >> update_unique_table_task
