import pandas as pd
import numpy as np
import sqlite3
import os
import zipfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi
from dateutil import parser

# Define default arguments
default_args = {
    'owner': 'Team_14',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'weather_data_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Weather Dataset with validation and trigger rules',
    schedule_interval='@daily',
    catchup=False
)

# File paths
db_path = '/home/vboxuser/airflow/databases/final.db'

# Task 1: Extract data
def download_weather_data(**kwargs):
    # Set environment variable for Kaggle API authentication
    # Make sure the correct path to the .kaggle directory is set where kaggle.json is located    
    # Initialize the Kaggle API client
    api = KaggleApi()
    api.authenticate()

    # Download the dataset file
    api.dataset_download_file('muthuj7/weather-dataset', file_name = 'weatherHistory.csv', path= '/home/vboxuser/airflow/weather-etl-pipeline/datasets')
    
    # file paths
    downloaded_file_path = '/home/vboxuser/airflow/weather-etl-pipeline/datasets/weatherHistory.csv'
    zip_file_path = downloaded_file_path + '.zip'


    # check if the file is actually zip file

    if zipfile.is_zipfile(zip_file_path):
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall('/home/vboxuser/airflow/weather-etl-pipeline/datasets')
        os.remove(zip_file_path)
    else:
        print('Downloaded file is NOT a zip file.')
   

 # Push the file path to XCom so that it can be used by the next task
    kwargs['ti'].xcom_push(key='csv_file_path', value = downloaded_file_path)

    return downloaded_file_path  # Optionally return the file path for logging or further debugging

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=download_weather_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Transform data
def transform_data(**kwargs):
    # Retrieve file path from XCom
    file_path = kwargs['ti'].xcom_pull(key='csv_file_path')
    if not file_path:
        raise ValueError("File path not found in XCom. Ensure that the extraction step has been completed successfully.")
    
    df = pd.read_csv(file_path)

    # Convert Formatted Date to a proper data format

    # Convert datatype into a datetime object (because this are strings)
    df['Formatted Date'] = df['Formatted Date'].apply(lambda x: parser.parse(x) if isinstance(x, str) else x)
    # Convert into pandas datetime64 object and remove timezone information (because different timezones used)
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], errors='coerce').dt.tz_localize(None)
    # Exctract the month and the date from the Formatted Date column
    df['Month'] = df['Formatted Date'].dt.month
    df['Formatted Date'] = df['Formatted Date'].dt.date


    # Handle missing values in critical columns
    critical_columns = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']
    for column in critical_columns:
        df[column].fillna(df[column].mean(), inplace=True)

    # Remove duplicate rows
    df.drop_duplicates(keep='last', inplace=True)

    # --- Feature Engineering --- #

    # Calculate daily average for weather data
    daily_averages = df.groupby('Formatted Date', as_index=False).agg({
        'Temperature (C)': 'mean',
        'Apparent Temperature (C)': 'mean',
        'Humidity': 'mean',
        'Wind Speed (km/h)': 'mean',
        'Visibility (km)': 'mean',
        'Pressure (millibars)': 'mean'
    })

    # Rename new columns
    daily_averages.rename(columns={
        'Formatted Date': 'formatted_date',
        'Temperature (C)': 'avg_temperature_c',
        'Apparent Temperature (C)': 'avg_apparent_temperature_c',
        'Humidity': 'avg_humidity',
        'Wind Speed (km/h)': 'avg_wind_speed_kmh',
        'Visibility (km)': 'avg_visibility_km',
        'Pressure (millibars)': 'avg_pressure_millibars'
    }, inplace=True)

    # Calculate monthly mode for Precip Type
    grouped_by_month = df.groupby('Month')['Precip Type']
    monthly_mode = grouped_by_month.agg(lambda x: x.mode()[0] if len(x.mode()) == 1 else np.nan).reset_index()
    monthly_mode.rename(columns={'Precip Type': 'Mode'}, inplace=True)

    # Add new feature 'wind_strength'
    # Convert Wind Speed from km/h to m/s
    daily_averages['avg_wind_speed_ms'] = daily_averages['avg_wind_speed_kmh'] * 1000 / 3600

    # Define bins and labels
    bins = [0, 1.5, 3.3, 5.4, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6, float('inf')]
    labels = [
        'Calm', 'Light Air', 'Light Breeze', 'Gentle Breeze', 'Moderate Breeze',
        'Fresh Breeze', 'Strong Breeze', 'Near Gale', 'Gale', 'Strong Gale',
        'Storm', 'Violent Storm'
    ]

    # Categorize wind strength
    daily_averages['wind_strength'] = pd.cut(daily_averages['avg_wind_speed_ms'], bins=bins, labels=labels)

    # Drop the temporary avg_wind_speed_ms
    daily_averages.drop(columns=['avg_wind_speed_ms'], inplace=True)

    # Calculate monthly averages 
    monthly_averages = df.groupby('Month', as_index=False).agg({
        'Temperature (C)': 'mean',
        'Apparent Temperature (C)': 'mean',
        'Humidity': 'mean',
        'Visibility (km)': 'mean',
        'Pressure (millibars)': 'mean'
    })

    # Rename new columns
    monthly_averages.rename(columns={
        'Temperature (C)': 'avg_temperature_c',
        'Apparent Temperature (C)': 'avg_apparent_temperature_c',
        'Humidity': 'avg_humidity',
        'Visibility (km)': 'avg_visibility_km',
        'Pressure (millibars)': 'avg_pressure_millibars'
    }, inplace=True)

    # Merge monthly mode for precip type into monthly averages
    monthly_averages = monthly_averages.merge(monthly_mode, on='Month', how='left')
    monthly_averages.rename(columns={'Mode': 'mode_precip_type'}, inplace=True)

    # Rename the 'Month' column in monthly_averages
    monthly_averages.rename(columns={
        'Month': 'month'
    }, inplace=True)

    # Save transformed data to new csv files
    daily_transformed_file_path = '/tmp/daily_data.csv'
    daily_averages.to_csv(daily_transformed_file_path, index=False)
    kwargs['ti'].xcom_push(key='daily_transformed_file_path', value=daily_transformed_file_path)

    monthly_transformed_file_path = '/tmp/monthly_data.csv'
    monthly_averages.to_csv(monthly_transformed_file_path, index=False)
    kwargs['ti'].xcom_push(key='monthly_transformed_file_path', value=monthly_transformed_file_path)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Validate data
def validate_data(**kwargs):
    # Retrieve transformed file pathes from XCom
    transformed_daily_file = kwargs['ti'].xcom_pull(key='daily_transformed_file_path')
    transformed_monthly_file = kwargs['ti'].xcom_pull(key='monthly_transformed_file_path')
    daily_averages = pd.read_csv(transformed_daily_file)
    monthly_averages = pd.read_csv(transformed_monthly_file)

    # Ensure no critical columns have missing values after transformation
    daily_critical_columns = ['avg_temperature_c', 'avg_humidity', 'avg_wind_speed_kmh', 'avg_apparent_temperature_c', 'avg_visibility_km', 'avg_pressure_millibars']
    if daily_averages[daily_critical_columns].isnull().any().any():
        raise ValueError("Validation failed: Missing values in critical columns for daily data after transformation.")
    
    monthly_critical_columns = ['avg_temperature_c', 'avg_humidity', 'avg_visibility_km', 'avg_pressure_millibars']
    if monthly_averages[monthly_critical_columns].isnull().any().any():
        raise ValueError("Validation failed: Missing values in critical columns for monthly data after transformation.")

    
    # Check whether the values are in appropriate ranges
    if (daily_averages['avg_temperature_c'] < -50).any() or (daily_averages['avg_temperature_c'] > 50).any():
        raise ValueError("Validation failed: Temperature values out of range.")
    if (monthly_averages['avg_temperature_c'] < -50).any() or (monthly_averages['avg_temperature_c'] > 50).any():
        raise ValueError("Validation failed: Temperature values out of range.")
    
    if (daily_averages['avg_humidity'] < 0).any() or (daily_averages['avg_humidity'] > 1).any():
        raise ValueError("Validation failed: Humidity values out of range.")
    if (monthly_averages['avg_humidity'] < 0).any() or (monthly_averages['avg_humidity'] > 1).any():
        raise ValueError("Validation failed: Humidity values out of range.")
    
    if (daily_averages['avg_wind_speed_kmh'] < 0).any():
        raise ValueError("Validation failed: Wind Speed values out of range.")
    

    # Check for critical outliers
    month_temperature_ranges = {   # define temperature ranges for each month
        1: (-50, 20),
        2: (-50, 20),
        3: (-40, 25),
        4: (-30, 30),
        5: (-20, 35),
        6: (-5, 35),
        7: (-1, 40),
        8: (0, 50),
        9: (-10, 50),
        10: (-20, 40),
        11: (-40, 30),
        12: (-50, 20)
    }
    
    daily_averages['Month'] = pd.to_datetime(daily_averages['formatted_date']).dt.month

    outliers = daily_averages[daily_averages.apply(lambda row: not(   # check for outliers
        month_temperature_ranges[row['Month']][0] <= row['avg_temperature_c'] <= month_temperature_ranges[row['Month']][1]
    ), axis=1)]

    if not outliers.empty:   # raise Error if outliers found
        raise ValueError(f"Validation failed: Outliers detected in avg_temperatures_c\n{outliers}")
    
    daily_averages.drop(columns=['Month'], inplace=True)   # drop the Month column since not needed anymore

    # Pass the files to XCom for the next task
    kwargs['ti'].xcom_push(key='validated_daily_data_path', value=transformed_daily_file)
    kwargs['ti'].xcom_push(key='validated_monthly_data_path', value=transformed_monthly_file)

validate_task = PythonOperator(
    task_id='validate_task',
    python_callable=validate_data,
    provide_context=True,
    trigger_rule='all_success',   # Only run validation task if previous tasks were completed successful
    dag=dag,
)

# Task 4: Load data
def load_data(**kwargs):
    # Retrieve validated file pathes from XCom
    validated_daily_data_file_path = kwargs['ti'].xcom_pull(key='validated_daily_data_path')
    validated_monthly_data_file_path = kwargs['ti'].xcom_pull(key='validated_monthly_data_path')

    daily_averages = pd.read_csv(validated_daily_data_file_path)
    monthly_averages = pd.read_csv(validated_monthly_data_file_path)

    # Load data into SQLite database
    conn = sqlite3.connect(db_path)  # if db doesn't exist, it will be created
    cursor = conn.cursor()

    # Create tables if they do not exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_weather (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT,
            "formatted_date" TEXT,
            "avg_temperature_c" FLOAT,
            "avg_apparent_temperature_c" FLOAT,
            "avg_humidity" FLOAT,
            "avg_wind_speed_kmh" FLOAT,
            "avg_visibility_km" FLOAT,
            "avg_pressure_millibars" FLOAT,
            "wind_strength" TEXT      
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monthly_weather (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT,
            "month" INTEGER,
            "avg_temperature_c" FLOAT,
            "avg_apparent_temperature_c" FLOAT,
            "avg_humidity" FLOAT,
            "avg_visibility_km" FLOAT,
            "avg_pressure_millibars" FLOAT,
            "mode_precip_type" TEXT      
        )
    ''')

    # Insert data into the database
    daily_averages.to_sql('daily_weather', conn, if_exists='replace', index=False)
    monthly_averages.to_sql('monthly_weather', conn, if_exists='replace', index=False)
    conn.commit()
    conn.cursor()
    conn.close()

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    trigger_rule='all_success',   # Only proceed to load task if validation was successful
    dag=dag,
)

# Set task dependencies with trigger rules
extract_task >> transform_task >> validate_task >> load_task