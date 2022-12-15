import json
from typing import List

import pandas as pd
from flair.data import Sentence, Span
from flair.models import SequenceTagger

tagger = SequenceTagger.load("flair/ner-german-large")

with open("./config.json") as f:
    CONFIG = json.load(f)


def __predict_sentence(row) -> List[Span]:  # type: ignore
    sentence = Sentence(row["text"])
    tagger.predict(sentence)
    return sentence.get_spans("ner")  # type: ignore


def main() -> None:
    df_updates = pd.read_csv(
        CONFIG["CLEANED_UPDATE_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    df_new = pd.read_csv(
        CONFIG["CLEANED_NEW_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )

    df_updates["flair_tags"] = df_updates.apply(__predict_sentence, axis=1)
    df_new["flair_tags"] = df_new.apply(__predict_sentence, axis=1)

    df_updates["flair_locations"] = df_updates["flair_tags"].apply(
        lambda x: ",".join([tag.text for tag in x if tag.tag == "LOC"])
    )
    df_new["flair_locations"] = df_new["flair_tags"].apply(
        lambda x: ",".join([tag.text for tag in x if tag.tag == "LOC"])
    )

    df_updates["flair_person"] = df_updates["flair_tags"].apply(
        lambda x: ",".join([tag.text for tag in x if tag.tag == "PER"])
    )
    df_new["flair_person"] = df_new["flair_tags"].apply(
        lambda x: ",".join([tag.text for tag in x if tag.tag == "PER"])
    )

    df_updates["flair_organizations"] = df_updates["flair_tags"].apply(
        lambda x: ",".join([tag.text for tag in x if tag.tag == "ORG"])
    )
    df_new["flair_organizations"] = df_new["flair_tags"].apply(
        lambda x: ",".join([tag.text for tag in x if tag.tag == "ORG"])
    )

    df_updates["flair_misc"] = df_updates["flair_tags"].apply(
        lambda x: ",".join([tag.text for tag in x if tag.tag == "MISC"])
    )
    df_new["flair_misc"] = df_new["flair_tags"].apply(
        lambda x: ",".join([tag.text for tag in x if tag.tag == "MISC"])
    )

    df_updates.drop(columns=["flair_tags"], inplace=True)
    df_new.drop(columns=["flair_tags"], inplace=True)

    df_updates.to_csv(CONFIG["FLAIR_UPDATE_CSV_PATH"], sep=";", index=False)
    df_new.to_csv(CONFIG["FLAIR_NEW_CSV_PATH"], sep=";", index=False)


if __name__ == "__main__":
    main()
