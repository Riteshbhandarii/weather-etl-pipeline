# Weather Data ETL Pipeline

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow to process weather data from a Kaggle dataset. The pipeline extracts weather data, transforms it by cleaning and aggregating, validates the data for consistency, and loads it into a SQLite database for further analysis.

## Prerequisites
- **Python**: Version 3.8 or higher
- **Apache Airflow**: Version 2.0 or higher
- **Kaggle API**: For downloading the dataset
- **Required Python Libraries**:
  - pandas
  - numpy
  - sqlite3
  - kaggle
  - python-dateutil
- **Directory Structure**:
  - `/home/artem-holovchak/airflow/datasets/`: For storing downloaded and extracted dataset files
  - `/home/artem-holovchak/airflow/databases/`: For storing the SQLite database
  - `/tmp/`: For temporary storage of transformed CSV files

## Installation
1. **Set up Airflow**:
   - Install Apache Airflow: `pip install apache-airflow`
   - Initialize the Airflow database: `airflow db init`
   - Start the Airflow scheduler and webserver:
     ```bash
     airflow scheduler &
     airflow webserver -p 8080
     ```

2. **Install Python dependencies**:
   ```bash
   pip install pandas numpy python-dateutil kaggle
   ```

3. **Configure Kaggle API**:
   - Obtain your Kaggle API token from the Kaggle website.
   - Place the `kaggle.json` file in `~/.kaggle/` or set up the Kaggle API credentials as environment variables.

4. **Set up directories**:
   ```bash
   mkdir -p /home/artem-holovchak/airflow/datasets
   mkdir -p /home/artem-holovchak/airflow/databases
   ```

5. **Place the DAG file**:
   - Copy the DAG script (`final_project_dag.py`) to your Airflow DAGs folder (e.g., `~/airflow/dags/`).

## Pipeline Description
The pipeline consists of four main tasks, orchestrated using Airflow:

1. **Extract Task**:
   - Downloads the `weatherHistory.csv` file from the Kaggle dataset `muthuj7/weather-dataset`.
   - Checks if the downloaded file is a ZIP archive and extracts it if necessary.
   - Stores the CSV file path in XCom for the next task.

2. **Transform Task**:
   - Reads the CSV file and performs data cleaning:
     - Converts `Formatted Date` to a proper datetime format, removes timezone information, and extracts the month.
     - Handles missing values in critical columns (e.g., Temperature, Humidity) by filling them with monthly means.
     - Removes duplicate rows.
   - Performs feature engineering:
     - Calculates daily averages for weather metrics (e.g., temperature, humidity).
     - Calculates monthly averages and the mode of `Precip Type`.
     - Adds a `wind_strength` feature based on wind speed categories.
   - Saves transformed daily and monthly data to temporary CSV files and pushes their paths to XCom.

3. **Validate Task**:
   - Validates the transformed data:
     - Ensures no missing values in critical columns.
     - Checks if values (e.g., temperature, humidity, wind speed) are within acceptable ranges.
     - Detects outliers in daily temperature data based on predefined monthly ranges and saves them to a CSV file if found.
   - Logs validation results (success or failure) and pushes validated file paths to XCom.

4. **Load Task**:
   - Loads the validated daily and monthly data into a SQLite database (`final_project.db`).
   - Creates two tables: `daily_weather` and `monthly_weather`, with appropriate schemas.
   - Replaces existing data in the tables with the new data.

## DAG Configuration
- **DAG ID**: `final_project_dag`
- **Schedule**: Runs daily (`@daily`)
- **Start Date**: November 19, 2024
- **Catchup**: Disabled
- **Retries**: 1 retry with a 1-minute delay
- **Trigger Rules**:
  - `validate_task` and `load_task` run only if all upstream tasks succeed (`all_success`).

## Task Dependencies
```plaintext
extract_task >> transform_task >> validate_task >> load_task
```

## Database Schema
### `daily_weather` Table
| Column                     | Type  | Description                              |
|----------------------------|-------|------------------------------------------|
| id                         | INTEGER | Primary key (auto-incremented)          |
| formatted_date             | TEXT  | Date of the weather data                |
| avg_temperature_c          | FLOAT | Average temperature (°C)                |
| avg_apparent_temperature_c | FLOAT | Average apparent temperature (°C)       |
| avg_humidity               | FLOAT | Average humidity (0-1)                  |
| avg_wind_speed_kmh         | FLOAT | Average wind speed (km/h)               |
| avg_visibility_km          | FLOAT | Average visibility (km)                 |
| avg_pressure_millibars     | FLOAT | Average pressure (millibars)            |
| wind_strength              | TEXT  | Wind strength category (e.g., Calm, Gale) |

### `monthly_weather` Table
| Column                     | Type  | Description                              |
|----------------------------|-------|------------------------------------------|
| id                         | INTEGER | Primary key (auto-incremented)          |
| month                      | INTEGER | Month number (1-12)                     |
| avg_temperature_c          | FLOAT | Average temperature (°C)                |
| avg_apparent_temperature_c | FLOAT | Average apparent temperature (°C)       |
| avg_humidity               | FLOAT | Average humidity (0-1)                  |
| avg_visibility_km          | FLOAT | Average visibility (km)                 |
| avg_pressure_millibars     | FLOAT | Average pressure (millibars)            |
| mode_precip_type           | TEXT  | Most common precipitation type          |

## Usage
1. **Run the DAG**:
   - Access the Airflow UI (default: `http://localhost:8080`).
   - Enable the `final_project_dag` and trigger it manually or wait for the scheduled run.
   - Monitor task execution in the Airflow UI.

2. **Check Outputs**:
   - **Dataset**: Downloaded and extracted CSV in `/home/artem-holovchak/airflow/datasets/`.
   - **Transformed Data**: Temporary CSV files in `/tmp/` (`daily_data.csv`, `monthly_data.csv`).
   - **Outliers**: If detected, saved as `outliers_daily_weather_<date>.csv` in `/home/artem-holovchak/airflow/datasets/`.
   - **Database**: SQLite database (`final_project.db`) in `/home/artem-holovchak/airflow/databases/`.

3. **Query the Database**:
   - Use SQLite to query the `daily_weather` and `monthly_weather` tables:
     ```bash
     sqlite3 /home/artem-holovchak/airflow/databases/final_project.db
     SELECT * FROM daily_weather LIMIT 10;
     SELECT * FROM monthly_weather;
     ```

## Troubleshooting
- **Kaggle API Errors**: Ensure the Kaggle API token is correctly configured.
- **File Path Issues**: Verify that the specified directories exist and are writable.
- **Validation Failures**: Check the Airflow logs for details on missing values or out-of-range data.
- **Database Errors**: Ensure the SQLite database path is accessible and not locked.

## Notes
- The pipeline assumes the Kaggle dataset (`muthuj7/weather-dataset`) is available and contains the expected columns.
- Outlier detection uses predefined temperature ranges for each month, which can be adjusted in the `validate_data` function.
- The pipeline overwrites existing data in the SQLite tables (`if_exists='replace'`).

## License
This project is licensed under the MIT License.
