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
    if not df_updates.empty:
        df_updates.to_sql("flair_output_updated_articles", engine, if_exists="replace")
    if not df_new.empty:
        df_new.to_sql("flair_output_new_articles", engine, if_exists="replace")
    if not df_gmaps_new.empty:
        df_gmaps_new.to_sql("gmaps_output_new_locations", engine, if_exists="replace")


def update_mv(config: Dict[str, str]) -> None:
    engine = create_engine(
        f"postgresql+psycopg2://postgres:{config['POSTGRES_PASSWORD']}@{config['POSTGRES_URL']}"
    )
    engine.execute("CALL public.update_staging_table_new()")
    engine.execute("CALL public.update_staging_table_existing()")
    engine.execute("CALL public.refresh_staging_mvs()")

    engine.execute("CALL dwh.update_dim_article()")
    engine.execute("CALL dwh.update_fact_focal_point()")
    engine.execute("CALL dwh.update_fact_influence()")
    engine.execute("CALL dwh.update_fact_spread()")
    engine.execute("CALL dwh.data_cleaning()")
