"""
2-hour error and response time analysis DAG
Analyzes service response times and error counts
Terminates pipeline if errors exceed threshold
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns

# Add plugins directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from database import get_db, DataRecord
from sqlalchemy import func

# Set plot style
sns.set_style('whitegrid')

ERROR_THRESHOLD = 10  # Maximum errors allowed in 2 hours


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def analyze_response_times(**context):
    """
    Task 3a: Analyze service response times over the last 2 hours
    """
    print("Analyzing response times...")

    # Get data from last 2 hours
    two_hours_ago = datetime.utcnow() - timedelta(hours=2)

    with get_db() as db:
        records = db.query(DataRecord).filter(
            DataRecord.parse_time >= two_hours_ago
        ).all()

        if not records:
            print("No data available for the last 2 hours")
            return None

        # Convert to DataFrame
        data_dicts = []
        for record in records:
            data_dicts.append({
                'parse_time': record.parse_time,
                'source1_response_time': record.source1_response_time,
                'source2_response_time': record.source2_response_time,
                'source3_response_time': record.source3_response_time,
                'source1_success': record.source1_success,
                'source2_success': record.source2_success,
                'source3_success': record.source3_success,
                'failed_requests': record.failed_requests
            })

        df = pd.DataFrame(data_dicts)

    print(f"\nAnalyzing {len(df)} records from the last 2 hours")

    # Response time statistics
    response_cols = ['source1_response_time', 'source2_response_time', 'source3_response_time']

    print("\nResponse Time Statistics:")
    for col in response_cols:
        if df[col].notna().sum() > 0:
            print(f"\n{col}:")
            print(f"  Mean: {df[col].mean():.4f}s")
            print(f"  Median: {df[col].median():.4f}s")
            print(f"  Min: {df[col].min():.4f}s")
            print(f"  Max: {df[col].max():.4f}s")
            print(f"  Std Dev: {df[col].std():.4f}s")

    # Error statistics
    total_errors = df['failed_requests'].sum()
    print(f"\nError Statistics:")
    print(f"  Total failed requests: {total_errors}")
    print(f"  Source 1 failures: {(~df['source1_success']).sum()}")
    print(f"  Source 2 failures: {(~df['source2_success']).sum()}")
    print(f"  Source 3 failures: {(~df['source3_success']).sum()}")
    print(f"  Success rate: {(1 - total_errors / (len(df) * 3)) * 100:.2f}%")

    # Save statistics
    execution_date = context['execution_date']
    timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
    output_dir = '/opt/airflow/data/error_analysis'
    os.makedirs(output_dir, exist_ok=True)

    stats_file = os.path.join(output_dir, f'response_stats_{timestamp}.txt')
    with open(stats_file, 'w') as f:
        f.write("Response Time and Error Analysis\n")
        f.write("=" * 60 + "\n")
        f.write(f"Analysis Period: Last 2 hours\n")
        f.write(f"Records Analyzed: {len(df)}\n")
        f.write(f"Time Range: {df['parse_time'].min()} to {df['parse_time'].max()}\n\n")

        f.write("Response Time Statistics:\n")
        f.write("-" * 60 + "\n")
        for col in response_cols:
            if df[col].notna().sum() > 0:
                f.write(f"\n{col}:\n")
                f.write(f"  Mean: {df[col].mean():.4f}s\n")
                f.write(f"  Median: {df[col].median():.4f}s\n")
                f.write(f"  Min: {df[col].min():.4f}s\n")
                f.write(f"  Max: {df[col].max():.4f}s\n")
                f.write(f"  Std Dev: {df[col].std():.4f}s\n")

        f.write("\nError Statistics:\n")
        f.write("-" * 60 + "\n")
        f.write(f"Total failed requests: {total_errors}\n")
        f.write(f"Source 1 failures: {(~df['source1_success']).sum()}\n")
        f.write(f"Source 2 failures: {(~df['source2_success']).sum()}\n")
        f.write(f"Source 3 failures: {(~df['source3_success']).sum()}\n")
        f.write(f"Success rate: {(1 - total_errors / (len(df) * 3)) * 100:.2f}%\n")

    print(f"\nStatistics saved to {stats_file}")

    # Return data for next task
    return {
        'df': df.to_dict('records'),
        'total_errors': int(total_errors)
    }


def create_error_plots(**context):
    """
    Task 3a: Create plots for response times and errors
    """
    print("Creating error analysis plots...")

    # Get data from previous task
    ti = context['task_instance']
    analysis_result = ti.xcom_pull(task_ids='analyze_response_times')

    if not analysis_result:
        print("No data available for plotting")
        return

    df = pd.DataFrame(analysis_result['df'])
    df['parse_time'] = pd.to_datetime(df['parse_time'])

    execution_date = context['execution_date']
    timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
    output_dir = '/opt/airflow/data/error_analysis'
    os.makedirs(output_dir, exist_ok=True)

    # Plot 1: Response times over time
    fig, axes = plt.subplots(3, 1, figsize=(12, 10))

    response_cols = ['source1_response_time', 'source2_response_time', 'source3_response_time']
    titles = ['Source 1 (Weather) Response Time', 'Source 2 (Currency) Response Time', 'Source 3 (Crypto) Response Time']

    for idx, (col, title) in enumerate(zip(response_cols, titles)):
        if df[col].notna().sum() > 0:
            axes[idx].plot(df['parse_time'], df[col], marker='o', linestyle='-', linewidth=2)
            axes[idx].set_title(title)
            axes[idx].set_xlabel('Time')
            axes[idx].set_ylabel('Response Time (s)')
            axes[idx].grid(True, alpha=0.3)
            axes[idx].tick_params(axis='x', rotation=45)

    plt.tight_layout()
    response_time_file = os.path.join(output_dir, f'response_times_{timestamp}.png')
    plt.savefig(response_time_file, dpi=100, bbox_inches='tight')
    plt.close()
    print(f"Response time plot saved: {response_time_file}")

    # Plot 2: Error distribution
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))

    # Failed requests over time
    axes[0, 0].plot(df['parse_time'], df['failed_requests'], marker='o', color='red', linewidth=2)
    axes[0, 0].set_title('Failed Requests Over Time')
    axes[0, 0].set_xlabel('Time')
    axes[0, 0].set_ylabel('Failed Requests')
    axes[0, 0].grid(True, alpha=0.3)
    axes[0, 0].tick_params(axis='x', rotation=45)

    # Error counts by source
    error_counts = [
        (~df['source1_success']).sum(),
        (~df['source2_success']).sum(),
        (~df['source3_success']).sum()
    ]
    axes[0, 1].bar(['Source 1', 'Source 2', 'Source 3'], error_counts, color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
    axes[0, 1].set_title('Error Count by Source')
    axes[0, 1].set_ylabel('Number of Failures')
    axes[0, 1].grid(True, alpha=0.3, axis='y')

    # Response time distribution (box plot)
    response_data = [
        df['source1_response_time'].dropna(),
        df['source2_response_time'].dropna(),
        df['source3_response_time'].dropna()
    ]
    axes[1, 0].boxplot(response_data, labels=['Source 1', 'Source 2', 'Source 3'])
    axes[1, 0].set_title('Response Time Distribution')
    axes[1, 0].set_ylabel('Response Time (s)')
    axes[1, 0].grid(True, alpha=0.3, axis='y')

    # Success rate pie chart
    total_requests = len(df) * 3
    total_errors = df['failed_requests'].sum()
    success_count = total_requests - total_errors
    axes[1, 1].pie([success_count, total_errors], labels=['Success', 'Failed'],
                   autopct='%1.1f%%', colors=['#4ECDC4', '#FF6B6B'], startangle=90)
    axes[1, 1].set_title('Overall Success Rate')

    plt.tight_layout()
    error_analysis_file = os.path.join(output_dir, f'error_analysis_{timestamp}.png')
    plt.savefig(error_analysis_file, dpi=100, bbox_inches='tight')
    plt.close()
    print(f"Error analysis plot saved: {error_analysis_file}")


def check_error_threshold(**context):
    """
    Task 3b: Check if error count exceeds threshold
    Returns task_id to branch to
    """
    ti = context['task_instance']
    analysis_result = ti.xcom_pull(task_ids='analyze_response_times')

    if not analysis_result:
        print("No analysis data available")
        return 'continue_pipeline'

    total_errors = analysis_result['total_errors']
    print(f"\nError Threshold Check:")
    print(f"  Total errors in last 2 hours: {total_errors}")
    print(f"  Threshold: {ERROR_THRESHOLD}")

    if total_errors > ERROR_THRESHOLD:
        print(f"  ⚠️  ERROR THRESHOLD EXCEEDED! Pipeline will be terminated.")
        return 'terminate_pipeline'
    else:
        print(f"  ✓ Error count within acceptable range. Pipeline continues.")
        return 'continue_pipeline'


def terminate_pipeline(**context):
    """
    Task 3b: Terminate pipeline if errors exceed threshold
    """
    ti = context['task_instance']
    analysis_result = ti.xcom_pull(task_ids='analyze_response_times')
    total_errors = analysis_result['total_errors'] if analysis_result else 0

    print(f"❌ PIPELINE TERMINATED")
    print(f"Reason: Error count ({total_errors}) exceeded threshold ({ERROR_THRESHOLD})")
    print(f"Manual intervention required to restart the pipeline.")

    # Log to file
    output_dir = '/opt/airflow/data/error_analysis'
    os.makedirs(output_dir, exist_ok=True)
    termination_file = os.path.join(output_dir, 'pipeline_termination.log')

    with open(termination_file, 'a') as f:
        f.write(f"\n{'=' * 60}\n")
        f.write(f"Pipeline Termination Event\n")
        f.write(f"Timestamp: {datetime.utcnow()}\n")
        f.write(f"Error Count: {total_errors}\n")
        f.write(f"Threshold: {ERROR_THRESHOLD}\n")
        f.write(f"{'=' * 60}\n")

    # Raise exception to mark DAG as failed
    raise AirflowException(f"Pipeline terminated: error count ({total_errors}) exceeded threshold ({ERROR_THRESHOLD})")


# Create the DAG
with DAG(
    'error_analysis_pipeline',
    default_args=default_args,
    description='2-hour error and response time analysis',
    schedule_interval='0 */2 * * *',  # Every 2 hours
    catchup=False,
    tags=['analysis', 'errors', 'homework']
) as dag:

    # Task 3a: Analyze response times and errors
    analyze_task = PythonOperator(
        task_id='analyze_response_times',
        python_callable=analyze_response_times
    )

    # Task 3a: Create plots
    plots_task = PythonOperator(
        task_id='create_error_plots',
        python_callable=create_error_plots
    )

    # Task 3b: Check error threshold (branching)
    check_threshold_task = BranchPythonOperator(
        task_id='check_error_threshold',
        python_callable=check_error_threshold
    )

    # Task 3b: Terminate if threshold exceeded
    terminate_task = PythonOperator(
        task_id='terminate_pipeline',
        python_callable=terminate_pipeline
    )

    # Continue normally if threshold not exceeded
    continue_task = EmptyOperator(
        task_id='continue_pipeline'
    )

    # Define dependencies
    analyze_task >> plots_task >> check_threshold_task
    check_threshold_task >> [terminate_task, continue_task]
