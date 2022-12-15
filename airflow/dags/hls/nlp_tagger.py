from typing import Dict, List

import pandas as pd
from flair.data import Sentence, Span
from flair.models import SequenceTagger

COLUMNS = [
    "HLS_ID",
    "HLS_URL",
    "first_name",
    "last_name",
    "birth_date",
    "death_date",
    "text",
    "published",
    "author",
    "translator",
    "hhb_ids",
    "hhb_forename",
    "hhb_surname",
    "hhb_sex",
    "hhb_birth_date",
    "hhb_death_date",
    "hhb_relation_id",
    "hhb_relation_name",
    "hhb_relation_type",
    "cleaned_birth_date_formatted",
    "cleaned_death_date_formatted",
    "flair_locations",
    "flair_person",
    "flair_organizations",
    "flair_misc",
]


def __flair_tags(tags: List[Span], type: str) -> str:
    """Extracts flair tags by LOC, PER, ORG, MISC and returns a string joined by comma."""
    return ",".join([tag.text for tag in tags if tag.tag == type])


def __predict_sentence(tagger, hls_entry) -> List[Span]:  # type: ignore
    """Predicts the sentence and returns the tags."""
    sentence = Sentence(hls_entry["text"])
    tagger.predict(sentence)
    return sentence.get_spans("ner")  # type: ignore


def __set_flair_tags(df: pd.DataFrame, index: int, spans: List[Span]) -> None:
    """Sets the flair tags to the dataframe."""
    df.loc[index, "flair_locations"] = __flair_tags(spans, "LOC")
    df.loc[index, "flair_person"] = __flair_tags(spans, "PER")
    df.loc[index, "flair_organisations"] = __flair_tags(spans, "ORG")
    df.loc[index, "flair_misc"] = __flair_tags(spans, "MISC")


def flair_tagger(config: Dict[str, str]) -> None:
    tagger = SequenceTagger.load("flair/ner-german-large")

    df_updates = pd.read_csv(
        config["CLEANED_UPDATE_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    df_new = pd.read_csv(
        config["CLEANED_NEW_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    df_updates[COLUMNS] = None
    df_new[COLUMNS] = None

    for index, hls_entry in df_updates.iterrows():
        spans = __predict_sentence(tagger, hls_entry)
        __set_flair_tags(df_updates, index, spans)

    for index, hls_entry in df_new.iterrows():
        spans = __predict_sentence(tagger, hls_entry)
        __set_flair_tags(df_updates, index, spans)

    df_updates.to_csv(config["FLAIR_UPDATE_CSV_PATH"], sep=";", index=False)
    df_new.to_csv(config["FLAIR_NEW_CSV_PATH"], sep=";", index=False)
