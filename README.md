# Airflow Pipeline for Data Warehouse and Data Lake Project (Secret Council)

This repository contains the code of our custom Airflow Pipeline.

## Project Structure

```
|--DWH_dags
    |--airflow\dags\                         # Airflow Dags
        |--config.json                       # File Locations, Gmaps API Token, PostgresInfo
        |--dag.py                            # Airflow DWH Dag

        |--hls\
            |--hls_scrape.py                 # 1. HLS-Scraping
            |--hhb_aggregation.py            # 2. Retrieve Hist-Hub Information
            |--hls_and_hhb_cleaning.py       # 3. Clean HLS and HHB Data
            |--nlp_tagger.py                 # 4. Flair Named Entity Recognition Model German Large
            |--flair_cleaning.py             # 5. Clean the Flair Model Output (Full Names and Locations)
            |--gmaps.py                      # 6. Retrieve Google Maps Information for Flair Locations
            |--data_base.py                  # 7. Push New and Updated Data to PostgresDB
            |--clean_up_temp.py              # 8. Remove temporary files and folders

    |--base\                                 # Base Data Folder provided so that people don't need to rescrape HLS from scratch
        |--hls_base.csv                      # Single Source of Truth of the HLS Scraping (15.10.2022)
        |--gmaps_base.csv                    # Google Maps Location Information (15.10.2022) 
```

## Development

```bash
# download a fresh python 3.9
conda create -n py39 python=3.9
# create a .venv inside the project and link against the Python 3.9 version installed through conda
poetry env use ~/.conda/envs/py39/bin/python
# install required packages defined in pyproject.toml into .venv
poetry install
# set up git hooks for autoformatting and linting (black, isort8, flake8) --> .pre-commit-config.yaml
pre-commit install
```

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