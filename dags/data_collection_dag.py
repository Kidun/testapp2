"""
Main DAG for data collection every 3 minutes
Collects data from 3 sources in parallel and stores in database
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

# Add plugins directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from database import get_db, init_db, DataRecord
from data_collectors import collect_weather_data, collect_currency_rates, collect_crypto_prices


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def init_iteration(**context):
    """Task 1a: Initialize iteration process"""
    execution_date = context['execution_date']
    print(f"Initializing data collection iteration at {execution_date}")
    # Initialize database if not exists
    init_db()
    print("Database initialized/verified")


def collect_source1(**context):
    """Task 1b: Collect data from source 1 (Weather)"""
    data, response_time, success = collect_weather_data()
    print(f"Source 1 (Weather) - Success: {success}, Time: {response_time:.2f}s")
    print(f"Data: {data}")
    # Store in XCom for aggregation
    return {
        'data': data,
        'response_time': response_time,
        'success': success
    }


def collect_source2(**context):
    """Task 1b: Collect data from source 2 (Currency)"""
    data, response_time, success = collect_currency_rates()
    print(f"Source 2 (Currency) - Success: {success}, Time: {response_time:.2f}s")
    print(f"Data: {data}")
    return {
        'data': data,
        'response_time': response_time,
        'success': success
    }


def collect_source3(**context):
    """Task 1b: Collect data from source 3 (Crypto)"""
    data, response_time, success = collect_crypto_prices()
    print(f"Source 3 (Crypto) - Success: {success}, Time: {response_time:.2f}s")
    print(f"Data: {data}")
    return {
        'data': data,
        'response_time': response_time,
        'success': success
    }


def save_to_database(**context):
    """
    Task 1c, 1d: Record metadata and save to database
    Aggregates data from all sources and saves to DB
    """
    ti = context['task_instance']

    # Pull data from XCom
    source1_result = ti.xcom_pull(task_ids='collect_source1')
    source2_result = ti.xcom_pull(task_ids='collect_source2')
    source3_result = ti.xcom_pull(task_ids='collect_source3')

    # Calculate failed requests count
    failed_requests = sum([
        not source1_result['success'],
        not source2_result['success'],
        not source3_result['success']
    ])

    # Prepare record
    record_data = {
        'parse_time': datetime.utcnow(),
        # Source 1: Weather
        **source1_result['data'],
        # Source 2: Currency
        **source2_result['data'],
        # Source 3: Crypto
        **source3_result['data'],
        # Metadata
        'failed_requests': failed_requests,
        'source1_response_time': source1_result['response_time'],
        'source2_response_time': source2_result['response_time'],
        'source3_response_time': source3_result['response_time'],
        'source1_success': source1_result['success'],
        'source2_success': source2_result['success'],
        'source3_success': source3_result['success']
    }

    # Save to database using context manager
    with get_db() as db:
        record = DataRecord(**record_data)
        db.add(record)
        db.commit()
        print(f"Record saved to database with ID: {record.id}")
        print(f"Failed requests: {failed_requests}")
        print(f"Parse time: {record.parse_time}")

    return record_data


# Create the DAG
with DAG(
    'data_collection_pipeline',
    default_args=default_args,
    description='Collect data from 3 sources every 3 minutes',
    schedule_interval='*/3 * * * *',  # Every 3 minutes
    catchup=False,
    tags=['data_collection', 'homework']
) as dag:

    # Task 1a: Initialize iteration
    init_task = PythonOperator(
        task_id='initialize_iteration',
        python_callable=init_iteration
    )

    # Task 1b: Collect from 3 sources in parallel
    collect_task1 = PythonOperator(
        task_id='collect_source1',
        python_callable=collect_source1
    )

    collect_task2 = PythonOperator(
        task_id='collect_source2',
        python_callable=collect_source2
    )

    collect_task3 = PythonOperator(
        task_id='collect_source3',
        python_callable=collect_source3
    )

    # Dummy task to wait for all parallel tasks
    wait_for_collection = EmptyOperator(
        task_id='wait_for_all_sources'
    )

    # Task 1c, 1d: Save to database with metadata
    save_task = PythonOperator(
        task_id='save_to_database',
        python_callable=save_to_database
    )

    # Define task dependencies
    # Initialize first
    init_task >> [collect_task1, collect_task2, collect_task3]

    # Collect in parallel, then wait for all to complete
    [collect_task1, collect_task2, collect_task3] >> wait_for_collection

    # Save to database after all data collected
    wait_for_collection >> save_task
