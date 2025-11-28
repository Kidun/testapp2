"""
Hourly descriptive analysis DAG
Analyzes collected data: statistics, distribution plots
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
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


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def calculate_statistics(**context):
    """
    Task 2a: Calculate basic statistics for all data columns
    """
    print("Calculating basic statistics...")

    # Get data from database
    with get_db() as db:
        records = db.query(DataRecord).all()

        if not records:
            print("No data available for analysis")
            return None

        # Convert to DataFrame
        data_dicts = []
        for record in records:
            data_dicts.append({
                'id': record.id,
                'parse_time': record.parse_time,
                'temperature': record.temperature,
                'humidity': record.humidity,
                'wind_speed': record.wind_speed,
                'usd_rate': record.usd_rate,
                'eur_rate': record.eur_rate,
                'gbp_rate': record.gbp_rate,
                'cny_rate': record.cny_rate,
                'btc_price': record.btc_price,
                'eth_price': record.eth_price,
                'stock_index': record.stock_index,
                'failed_requests': record.failed_requests
            })

        df = pd.DataFrame(data_dicts)

    # Calculate statistics for numeric columns
    numeric_cols = [
        'temperature', 'humidity', 'wind_speed',
        'usd_rate', 'eur_rate', 'gbp_rate', 'cny_rate',
        'btc_price', 'eth_price', 'stock_index'
    ]

    stats = df[numeric_cols].describe()
    print("\nDescriptive Statistics:")
    print(stats)

    # Calculate additional statistics
    print("\nAdditional Statistics:")
    for col in numeric_cols:
        if df[col].notna().sum() > 0:  # Check if column has data
            print(f"\n{col}:")
            print(f"  Median: {df[col].median():.2f}")
            print(f"  Skewness: {df[col].skew():.4f}")
            print(f"  Kurtosis: {df[col].kurtosis():.4f}")
            print(f"  Missing values: {df[col].isna().sum()}")

    # Save statistics to file
    execution_date = context['execution_date']
    timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
    output_dir = '/opt/airflow/data/hourly_analysis'
    os.makedirs(output_dir, exist_ok=True)

    stats_file = os.path.join(output_dir, f'statistics_{timestamp}.csv')
    stats.to_csv(stats_file)
    print(f"\nStatistics saved to {stats_file}")

    return df.to_dict('records')


def plot_distributions(**context):
    """
    Task 2b: Create distribution plots for each data column
    """
    print("Creating distribution plots...")

    # Get data from database
    with get_db() as db:
        records = db.query(DataRecord).all()

        if not records:
            print("No data available for plotting")
            return

        # Convert to DataFrame
        data_dicts = []
        for record in records:
            data_dicts.append({
                'temperature': record.temperature,
                'humidity': record.humidity,
                'wind_speed': record.wind_speed,
                'usd_rate': record.usd_rate,
                'eur_rate': record.eur_rate,
                'gbp_rate': record.gbp_rate,
                'cny_rate': record.cny_rate,
                'btc_price': record.btc_price,
                'eth_price': record.eth_price,
                'stock_index': record.stock_index
            })

        df = pd.DataFrame(data_dicts)

    # Create distribution plots
    numeric_cols = [
        'temperature', 'humidity', 'wind_speed',
        'usd_rate', 'eur_rate', 'gbp_rate', 'cny_rate',
        'btc_price', 'eth_price', 'stock_index'
    ]

    execution_date = context['execution_date']
    timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
    output_dir = '/opt/airflow/data/hourly_analysis'
    os.makedirs(output_dir, exist_ok=True)

    # Create individual plots for each column
    for col in numeric_cols:
        if df[col].notna().sum() > 0:  # Check if column has data
            fig, axes = plt.subplots(1, 2, figsize=(12, 4))

            # Histogram
            axes[0].hist(df[col].dropna(), bins=20, edgecolor='black', alpha=0.7)
            axes[0].set_title(f'{col} - Histogram')
            axes[0].set_xlabel(col)
            axes[0].set_ylabel('Frequency')
            axes[0].grid(True, alpha=0.3)

            # Box plot
            axes[1].boxplot(df[col].dropna())
            axes[1].set_title(f'{col} - Box Plot')
            axes[1].set_ylabel(col)
            axes[1].grid(True, alpha=0.3)

            plt.tight_layout()
            plot_file = os.path.join(output_dir, f'{col}_distribution_{timestamp}.png')
            plt.savefig(plot_file, dpi=100, bbox_inches='tight')
            plt.close()
            print(f"Plot saved: {plot_file}")

    # Create a combined overview plot
    fig, axes = plt.subplots(4, 3, figsize=(15, 16))
    axes = axes.flatten()

    for idx, col in enumerate(numeric_cols):
        if df[col].notna().sum() > 0:
            axes[idx].hist(df[col].dropna(), bins=15, edgecolor='black', alpha=0.7, color='steelblue')
            axes[idx].set_title(col)
            axes[idx].grid(True, alpha=0.3)

    # Hide unused subplots
    for idx in range(len(numeric_cols), len(axes)):
        axes[idx].axis('off')

    plt.tight_layout()
    overview_file = os.path.join(output_dir, f'overview_distributions_{timestamp}.png')
    plt.savefig(overview_file, dpi=100, bbox_inches='tight')
    plt.close()
    print(f"Overview plot saved: {overview_file}")


def save_analysis_summary(**context):
    """
    Task 2c: Create and save analysis summary
    """
    print("Saving analysis summary...")

    execution_date = context['execution_date']
    timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
    output_dir = '/opt/airflow/data/hourly_analysis'

    # Get record count
    with get_db() as db:
        record_count = db.query(func.count(DataRecord.id)).scalar()

    # Create summary file
    summary_file = os.path.join(output_dir, f'analysis_summary_{timestamp}.txt')
    with open(summary_file, 'w') as f:
        f.write(f"Hourly Analysis Summary\n")
        f.write(f"=" * 50 + "\n")
        f.write(f"Execution Date: {execution_date}\n")
        f.write(f"Total Records Analyzed: {record_count}\n")
        f.write(f"\nFiles Generated:\n")
        f.write(f"  - statistics_{timestamp}.csv\n")
        f.write(f"  - overview_distributions_{timestamp}.png\n")
        f.write(f"  - Individual distribution plots for each column\n")
        f.write(f"\nAnalysis completed successfully.\n")

    print(f"Summary saved to {summary_file}")
    print(f"All analysis files saved to {output_dir}")


# Create the DAG
with DAG(
    'hourly_analysis_pipeline',
    default_args=default_args,
    description='Hourly descriptive analysis of collected data',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    catchup=False,
    tags=['analysis', 'hourly', 'homework']
) as dag:

    # Task 2a: Calculate statistics
    stats_task = PythonOperator(
        task_id='calculate_statistics',
        python_callable=calculate_statistics
    )

    # Task 2b: Create distribution plots
    plots_task = PythonOperator(
        task_id='plot_distributions',
        python_callable=plot_distributions
    )

    # Task 2c: Save summary
    summary_task = PythonOperator(
        task_id='save_analysis_summary',
        python_callable=save_analysis_summary
    )

    # Define dependencies
    stats_task >> plots_task >> summary_task
