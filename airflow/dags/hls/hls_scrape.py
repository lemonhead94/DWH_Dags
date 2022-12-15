import json
import pathlib
from datetime import date
from typing import Dict

import hlsscraper


def __override_config(config: Dict[str, str]) -> None:
    config_path = pathlib.Path(__file__).parent.resolve() / "config.json"
    config["LAST_SCRAPING_DATE"] = date.today().strftime("%d.%m.%Y")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=4)


def scrape_hls(config: Dict[str, str]) -> None:
    hlsscraper.scrape(
        base_csv_path=config["HLS_BASE_CSV_PATH"],
        update_csv_path=config["HLS_UPDATE_CSV_PATH"],
        new_csv_path=config["HLS_NEW_CSV_PATH"],
        last_scraping=config["LAST_SCRAPING_DATE"],
        crawl_delay=0,  # remove delay for testing
    )
    __override_config(config)
