# Installation

## Airflow Setup

````bash
export AIRFLOW_HOME="/opt/airflow"
````

## Config JSON

Modify the `/opt/airflow/dags/config.json`:

````bash
{
    "LAST_SCRAPING_DATE": "15.12.2022",
    "HLS_BASE_CSV_PATH": "/opt/airflow/base/hls_base.csv",
    "GMAPS_BASE_CSV_PATH": "/opt/airflow/base/gmaps_base.csv",
    "HLS_UPDATE_CSV_PATH": "/opt/airflow/data/hls_updates.csv",
    "HLS_NEW_CSV_PATH": "/opt/airflow/data/hls_new.csv",
    "HHB_UPDATE_CSV_PATH": "/opt/airflow/data/hhb_updates.csv",
    "HHB_NEW_CSV_PATH": "/opt/airflow/data/hhb_new.csv",
    "CLEANED_UPDATE_CSV_PATH": "/opt/airflow/data/cleaned_updates.csv",
    "CLEANED_NEW_CSV_PATH": "/opt/airflow/data/cleaned_new.csv",
    "FLAIR_UPDATE_CSV_PATH": "/opt/airflow/data/flair_updates.csv",
    "FLAIR_NEW_CSV_PATH": "/opt/airflow/data/flair_new.csv",
    "FLAIR_CLEANED_UPDATE_CSV_PATH": "/opt/airflow/data/flair_cleaned_updates.csv",
    "FLAIR_CLEANED_NEW_CSV_PATH": "/opt/airflow/data/flair_cleaned_new.csv",
    "GMAPS_NEW_CSV_PATH": "/opt/airflow/data/gmaps_new.csv",
    "POSTGRES_URL": "test.aiforge.ch/postgres",
    "POSTGRES_PASSWORD": "XXX",
    "GMAPS_API_TOKEN": "XXX"
}
````