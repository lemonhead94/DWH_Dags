import json
import pathlib
from datetime import datetime
from functools import partial

from hls import (
    clean_flair,
    clean_hls_and_hhb,
    flair_tagger,
    get_hist_hub_entries,
    gmaps_geocode,
    scrape_hls,
)

from airflow.models import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="hls_dag",
    start_date=datetime(2022, 12, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    config_path = pathlib.Path(__file__).parent.resolve() / "config.json"
    with open(config_path) as f:
        CONFIG = json.load(f)

    scrape_hls_task = PythonOperator(
        task_id="scrape_hls_task", python_callable=partial(scrape_hls, config=CONFIG)
    )
    aggregate_hhb_task = PythonOperator(
        task_id="aggregate_hhb_task",
        python_callable=partial(get_hist_hub_entries, config=CONFIG),
    )
    cleaning_task = PythonOperator(
        task_id="cleaning_task",
        python_callable=partial(clean_hls_and_hhb, config=CONFIG),
    )
    flair_tagger_task = PythonOperator(
        task_id="flair_tagger_task",
        python_callable=partial(flair_tagger, config=CONFIG),
    )
    clean_flair_task = PythonOperator(
        task_id="clean_flair_task", python_callable=partial(clean_flair, config=CONFIG)
    )
    gmaps_task = PythonOperator(
        task_id="gmaps_task", python_callable=partial(gmaps_geocode, config=CONFIG)
    )

    (
        scrape_hls_task
        >> aggregate_hhb_task
        >> cleaning_task
        >> flair_tagger_task
        >> clean_flair_task
        >> gmaps_task
    )
