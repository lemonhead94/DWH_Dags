import glob
import os


def clean_up_temp() -> None:
    files = glob.glob("/opt/airflow/data/temp/*")
    for f in files:
        os.remove(f)
