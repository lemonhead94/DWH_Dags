import json
import pathlib
from datetime import date
from pathlib import Path
from typing import Dict

import hlsscraper
import pandas as pd


def __override_config(config: Dict[str, str]) -> None:
    config_path = pathlib.Path(__file__).parent.parent.resolve() / "config.json"
    config["LAST_SCRAPING_DATE"] = date.today().strftime("%d.%m.%Y")
    with open(config_path, "w") as f:
        json.dump(obj=config, fp=f, indent=4)


def scrape_hls(config: Dict[str, str]) -> bool:
    hlsscraper.scrape(
        base_csv_path=config["HLS_BASE_CSV_PATH"],
        update_csv_path=config["HLS_UPDATE_CSV_PATH"],
        new_csv_path=config["HLS_NEW_CSV_PATH"],
        last_scraping=config["LAST_SCRAPING_DATE"],
    )
    __override_config(config)

    # check if there are any new HLS entries and return True to continue the DAG
    if Path(config["HLS_UPDATE_CSV_PATH"]).is_file():
        return not pd.read_csv(config["HLS_UPDATE_CSV_PATH"], sep=";").empty

    if Path(config["HLS_NEW_CSV_PATH"]).is_file():
        return not pd.read_csv(config["HLS_NEW_CSV_PATH"], sep=";").empty

    return False
