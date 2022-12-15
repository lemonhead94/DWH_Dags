import json
from typing import Dict, List

import googlemaps
import pandas as pd

with open("./config.json") as f:
    CONFIG = json.load(f)

gmaps = googlemaps.Client(key=CONFIG["GMAPS_API_TOKEN"])


def __get_gmaps_dataframe(locations: List[str]) -> pd.DataFrame:
    """get location data from gmaps into a dataframe and return it"""
    df = pd.DataFrame()
    for location in locations:
        geocode_result = gmaps.geocode(location)
        if len(geocode_result) > 0:
            address_components: Dict[str, str] = {}
            for component in geocode_result[0]["address_components"]:
                if len(component["types"]) > 0:
                    address_components[
                        f"gmaps_{component['types'][0]}_long_name"
                    ] = component["long_name"]
                    address_components[
                        f"gmaps_{component['types'][0]}_short_name"
                    ] = component["short_name"]
                else:
                    address_components["gmaps_long_name"] = component["long_name"]
                    address_components["gmaps_short_name"] = component["short_name"]

            df = pd.concat(
                [
                    df,
                    pd.DataFrame(
                        [
                            {
                                "flair_location_name": location,
                                "gmaps_place_id": geocode_result[0]["place_id"],
                                "gmaps_location_type": geocode_result[0]["geometry"][
                                    "location_type"
                                ],
                                "gmaps_location_lat": geocode_result[0]["geometry"][
                                    "location"
                                ]["lat"],
                                "gmaps_location_lng": geocode_result[0]["geometry"][
                                    "location"
                                ]["lng"],
                                "gmaps_formatted_address": geocode_result[0][
                                    "formatted_address"
                                ],
                                **address_components,
                            }
                        ]
                    ),
                ]
            )
    return df


def __get_new_locations(locations: List[str], gmaps_base: pd.DataFrame) -> List[str]:
    """Get new locations"""
    new_locations = []
    for location in locations:
        if not gmaps_base["flair_location_name"].str.contains(location).any():
            new_locations.append(location)
    return new_locations


def main() -> None:

    df_gmaps_base = pd.read_csv(CONFIG["GMAPS_BASE_CSV_PATH"], sep=";")

    df_updates = pd.read_csv(
        CONFIG["FLAIR_CLEANED_UPDATE_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    df_new = pd.read_csv(
        CONFIG["FLAIR_CLEANED_NEW_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    flair_location_updates: List[str] = __get_new_locations(
        locations=df_updates["flair_locations"].str.split(",").explode().unique(),
        gmaps_base=df_gmaps_base,
    )
    flair_location_new: List[str] = __get_new_locations(
        locations=df_new["flair_locations"].str.split(",").explode().unique(),
        gmaps_base=df_gmaps_base,
    )

    df_gmaps_updates = __get_gmaps_dataframe(locations=flair_location_updates)
    df_gmaps_new = __get_gmaps_dataframe(locations=flair_location_new)
    df_gmaps_new_entries = pd.concat([df_gmaps_updates, df_gmaps_new])

    df_gmaps_new_entries.to_csv(CONFIG["GMAPS_NEW_CSV_PATH"], sep=";", index=False)

    # update gmaps base with new location data
    df_gmaps_base = pd.concat([df_gmaps_base, df_gmaps_new_entries])
    df_gmaps_base.to_csv(CONFIG["GMAPS_BASE_CSV_PATH"], sep=";", index=False)


if __name__ == "__main__":
    main()
