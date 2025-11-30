"""
Airflow DAG to fetch hourly weather data from Open-Meteo API.
Fetches weather data for all outlets for the last 24 hours.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from fetch_weather import weather_fetch_pipeline

dag = DAG(
    'fetch_weather_data',
    description='Fetch hourly weather data for all outlets',
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather', 'api', 'hourly'],
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
)

fetch_weather = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=weather_fetch_pipeline,
    dag=dag,
)
