from typing import Dict, List

import pandas as pd
from flair.data import Sentence, Span
from flair.models import SequenceTagger

COLUMNS = [
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
    # add new empy columns since these columns are expected in the next task
    df_updates[COLUMNS] = pd.Series(dtype=pd.StringDtype())
    df_new[COLUMNS] = pd.Series(dtype=pd.StringDtype())

    for index, hls_entry in df_updates.iterrows():
        spans = __predict_sentence(tagger, hls_entry)
        __set_flair_tags(df_updates, index, spans)

    for index, hls_entry in df_new.iterrows():
        spans = __predict_sentence(tagger, hls_entry)
        __set_flair_tags(df_updates, index, spans)

    df_updates.to_csv(config["FLAIR_UPDATE_CSV_PATH"], sep=";", index=False)
    df_new.to_csv(config["FLAIR_NEW_CSV_PATH"], sep=";", index=False)
