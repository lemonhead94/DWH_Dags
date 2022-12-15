import json
from datetime import date

import hlsscraper

with open("./config.json") as f:
    CONFIG = json.load(f)


def main() -> None:
    hlsscraper.scrape(
        base_csv_path=CONFIG["HLS_BASE_CSV_PATH"],
        update_csv_path=CONFIG["HLS_UPDATE_CSV_PATH"],
        new_csv_path=CONFIG["HLS_NEW_CSV_PATH"],
        last_scraping=CONFIG["LAST_SCRAPING_DATE"],
        crawl_delay=0,  # remove delay for testing
    )
    CONFIG["LAST_SCRAPING_DATE"] = date.today().strftime("%d.%m.%Y")
    with open("./config.json", "w") as f:
        json.dump(CONFIG, f, indent=4)


if __name__ == "__main__":
    main()
