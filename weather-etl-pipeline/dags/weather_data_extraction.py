import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
from airflow.models import Variable

def download_weather_data(**kwargs):
    # Set environment variable for Kaggle API authentication
    # Make sure the correct path to the .kaggle directory is set where kaggle.json is located
    os.environ['KAGGLE_CONFIG_DIR'] = '/home/airflow/weather-etl-pipeline/.kaggle'  # Path to .kaggle folder
    
    # Initialize the Kaggle API client
    api = KaggleApi()
    api.authenticate()  # Authenticate using kaggle.json found in the .kaggle folder

    # Define the dataset name and destination path
    dataset_name = 'kaggle/weather-history'  # Replace with actual Kaggle dataset name
    download_dir = '/home/airflow/weather-etl-pipeline/datasets/'  # Folder where you want to store the zip file and extracted data
    
    # Make sure the download directory exists
    os.makedirs(download_dir, exist_ok=True)
    
    # Define the path for the downloaded zip file
    download_path = os.path.join(download_dir, 'weather_data.zip')

    # Download the dataset (This will download the dataset as a zip file)
    api.dataset_download_files(dataset_name, path=download_dir, unzip=False)
    
    # Check if the downloaded file exists
    if not os.path.exists(download_path):
        raise FileNotFoundError(f"Downloaded zip file not found at {download_path}")
    
    # Unzip the downloaded file (if it's in zip format)
    with zipfile.ZipFile(download_path, 'r') as zip_ref:
        zip_ref.extractall(download_dir)  # Extract all files to the download directory
   
    file_path = os.path.join(download_dir, 'weatherHistory.csv')  
    
    # Check if the extracted CSV file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found at {file_path}")
    
 # Push the file path to XCom so that it can be used by the next task
    kwargs['ti'].xcom_push(key='weather_data_file', value=file_path)

    return file_path  # Optionally return the file path for logging or further debugging


