from typing import Dict, List

import googlemaps
import pandas as pd
from googlemaps import Client

COLUMNS = [
    "flair_location_name",
    "gmaps_place_id",
    "gmaps_location_type",
    "gmaps_location_lat",
    "gmaps_location_lng",
    "gmaps_formatted_address",
    "gmaps_locality_long_name",
    "gmaps_locality_short_name",
    "gmaps_administrative_area_level_3_long_name",
    "gmaps_administrative_area_level_3_short_name",
    "gmaps_administrative_area_level_2_long_name",
    "gmaps_administrative_area_level_2_short_name",
    "gmaps_administrative_area_level_1_long_name",
    "gmaps_administrative_area_level_1_short_name",
    "gmaps_country_long_name",
    "gmaps_country_short_name",
    "gmaps_postal_code_long_name",
    "gmaps_postal_code_short_name",
    "gmaps_postal_town_long_name",
    "gmaps_postal_town_short_name",
    "gmaps_neighborhood_long_name",
    "gmaps_neighborhood_short_name",
    "gmaps_political_long_name",
    "gmaps_political_short_name",
    "gmaps_colloquial_area_long_name",
    "gmaps_colloquial_area_short_name",
    "gmaps_establishment_long_name",
    "gmaps_establishment_short_name",
    "gmaps_street_number_long_name",
    "gmaps_street_number_short_name",
    "gmaps_route_long_name",
    "gmaps_route_short_name",
    "gmaps_postal_code_suffix_long_name",
    "gmaps_postal_code_suffix_short_name",
    "gmaps_subpremise_long_name",
    "gmaps_subpremise_short_name",
    "gmaps_continent_long_name",
    "gmaps_continent_short_name",
    "gmaps_premise_long_name",
    "gmaps_premise_short_name",
    "gmaps_airport_long_name",
    "gmaps_airport_short_name",
    "gmaps_administrative_area_level_4_long_name",
    "gmaps_administrative_area_level_4_short_name",
    "gmaps_long_name",
    "gmaps_short_name",
    "gmaps_archipelago_long_name",
    "gmaps_archipelago_short_name",
    "gmaps_intersection_long_name",
    "gmaps_intersection_short_name",
    "gmaps_plus_code_long_name",
    "gmaps_plus_code_short_name",
    "gmaps_art_gallery_long_name",
    "gmaps_art_gallery_short_name",
    "gmaps_administrative_area_level_5_long_name",
    "gmaps_administrative_area_level_5_short_name",
    "gmaps_amusement_park_long_name",
    "gmaps_amusement_park_short_name",
    "gmaps_church_long_name",
    "gmaps_church_short_name",
    "gmaps_bar_long_name",
    "gmaps_bar_short_name",
    "gmaps_town_square_long_name",
    "gmaps_town_square_short_name",
    "gmaps_cafe_long_name",
    "gmaps_cafe_short_name",
    "gmaps_landmark_long_name",
    "gmaps_landmark_short_name",
    "gmaps_bus_station_long_name",
    "gmaps_bus_station_short_name",
    "gmaps_campground_long_name",
    "gmaps_campground_short_name",
]


def __get_gmaps_dataframe(locations: List[str], gmaps: Client) -> pd.DataFrame:
    """get location data from gmaps into a dataframe and return it"""
    df = pd.DataFrame(columns=COLUMNS)
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


def gmaps_geocode(config: Dict[str, str]) -> None:

    df_gmaps_base = pd.read_csv(config["GMAPS_BASE_CSV_PATH"], sep=";")
    gmaps = googlemaps.Client(key=config["GMAPS_API_TOKEN"])

    df_updates = pd.read_csv(
        config["FLAIR_CLEANED_UPDATE_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    df_new = pd.read_csv(
        config["FLAIR_CLEANED_NEW_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    flair_location_updates: List[str] = __get_new_locations(
        locations=df_updates["flair_locations"].str.split(",").explode().unique(),
        gmaps_base=df_gmaps_base,
    )
    flair_location_new: List[str] = __get_new_locations(
        locations=df_new["flair_locations"].str.split(",").explode().unique(),
        gmaps_base=df_gmaps_base,
    )

    df_gmaps_updates = __get_gmaps_dataframe(
        locations=flair_location_updates, gmaps=gmaps
    )
    df_gmaps_new = __get_gmaps_dataframe(locations=flair_location_new, gmaps=gmaps)
    df_gmaps_new_entries = pd.concat([df_gmaps_updates, df_gmaps_new])

    df_gmaps_new_entries.to_csv(config["GMAPS_NEW_CSV_PATH"], sep=";", index=False)

    # update gmaps base with new location data
    df_gmaps_base = pd.concat([df_gmaps_base, df_gmaps_new_entries])
    df_gmaps_base.to_csv(config["GMAPS_BASE_CSV_PATH"], sep=";", index=False)
