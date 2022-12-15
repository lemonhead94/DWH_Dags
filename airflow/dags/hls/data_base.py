from typing import Dict

import pandas as pd
from sqlalchemy import create_engine


def push_to_postgres(config: Dict[str, str]) -> None:
    engine = create_engine(
        f"postgresql+psycopg2://postgres:{config['POSTGRES_PASSWORD']}@{config['POSTGRES_URL']}"
    )

    df_updates = pd.read_csv(
        config["FLAIR_CLEANED_UPDATE_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    df_new = pd.read_csv(
        config["FLAIR_CLEANED_NEW_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    df_gmaps_new = pd.read_csv(
        config["GMAPS_NEW_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    df_updates.to_sql("flair_output_updated_articles.csv", engine, if_exists="replace")
    df_new.to_sql("flair_output_new_articles.csv", engine, if_exists="replace")
    df_gmaps_new.to_sql("gmaps_output_new_locations.csv", engine, if_exists="replace")


def update_mv(config: Dict[str, str]) -> None:
    engine = create_engine(
        f"postgresql+psycopg2://postgres:{config['POSTGRES_PASSWORD']}@{config['POSTGRES_URL']}"
    )
    engine.execute("CALL public.refresh_staging_mvs()")
