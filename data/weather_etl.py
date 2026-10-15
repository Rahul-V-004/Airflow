from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_transform():
    """
    Extracts weather data for Bengaluru from the free Open-Meteo API
    and transforms it into a structured dictionary.
    """
    # Open-Meteo API endpoint for Bengaluru coordinates
    url = "https://api.open-meteo.com/v1/forecast?latitude=12.9716&longitude=77.5946&current=temperature_2m,wind_speed_10m,weather_code"
    
    response = requests.get(url)
    response.raise_for_status() # Raise exception for bad status codes
    data = response.json()
    
    current_weather = data['current']
    
    # Transform data fields
    transformed_data = {
        'weather_time': current_weather['time'].replace('T', ' '), # Converts 'YYYY-MM-DDTHH:MM' to MySQL DATETIME format
        'temperature': current_weather['temperature_2m'],
        'wind_speed': current_weather['wind_speed_10m'],
        'weather_code': current_weather['weather_code']
    }
    
    return transformed_data

def load_to_mysql(ti):
    """
    Pulls transformed data from XCom and loads it into the MySQL database.
    """
    # Pull data from the previous task
    weather_data = ti.xcom_pull(task_ids='extract_transform_weather')
    
    # Initialize MySQL Hook (Uses connection ID defined in Airflow UI)
    mysql_hook = MySqlHook(mysql_conn_id='mysql_weather_conn')
    
    sql_insert = """
        INSERT INTO bengaluru_weather (weather_time, temperature_celsius, wind_speed_kmh, weather_code)
        VALUES (%s, %s, %s, %s);
    """
    
    target_data = (
        weather_data['weather_time'],
        weather_data['temperature'],
        weather_data['wind_speed'],
        weather_data['weather_code']
    )
    
    # Execute the insert statement
    mysql_hook.run(sql_insert, parameters=target_data)


# Define the DAG
with DAG(
    dag_id='bengaluru_weather_etl_pipeline',
    default_args=default_args,
    description='An hourly ETL pipeline for Bengaluru weather data',
    schedule_interval='@hourly',
    catchup=False,
    tags=['weather', 'etl']
) as dag:

    extract_transform_task = PythonOperator(
        task_id='extract_transform_weather',
        python_callable=extract_and_transform
    )

    load_task = PythonOperator(
        task_id='load_weather_to_mysql',
        python_callable=load_to_mysql
    )

    # Set dependency flow
    extract_transform_task >> load_task
