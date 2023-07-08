# Author: Anuj Chauhan

# history_text is a ordered list of previous questions in the same interaction.

import os
import json
import shutil

OG_DIR = "original/sparc/"
OUTPUT_DIR = "unified/sparc/"

os.makedirs(OUTPUT_DIR)

INPUT_FILES = os.listdir(OG_DIR)

INPUT_DEV_FILE = [file for file in INPUT_FILES if file.endswith("dev.json")][0]
INPUT_TRAIN_FILE = [
    file for file in INPUT_FILES if file.endswith("train.json")
][0]

FILES_TO_CONVERT = [INPUT_DEV_FILE, INPUT_TRAIN_FILE]

for file in FILES_TO_CONVERT:
    OUTPUT_FILE = (
        f"{OUTPUT_DIR}dev.jsonl"
        if "dev" in file
        else f"{OUTPUT_DIR}train.jsonl"
    )

    input_data = json.load(open(os.path.join(OG_DIR, file)))

    for entry in input_data:
        for idx, datum in enumerate(entry["interaction"]):
            unified_json_entry = {}
            unified_json_entry["db_id"] = entry["database_id"]
            unified_json_entry["question"] = datum["utterance"]
            unified_json_entry["query"] = datum["query"]

            # OperationalError('unrecognized token: "! "')
            # OperationalError('near "=": syntax error')
            unified_json_entry["query"] = (
                unified_json_entry["query"]
                .replace("! =", "!=")
                .replace("> =", ">=")
                .replace("< =", "<=")
            )

            unified_json_entry["question"] = datum["utterance"]

            unified_json_entry["history_text"] = [
                seeker["utterance"] for seeker in entry["interaction"][:idx]
            ]

            with open(OUTPUT_FILE, "a+") as writer:
                json.dump(unified_json_entry, writer)
                writer.write("\n")

shutil.copy(OG_DIR + "tables.json", OUTPUT_DIR + "tables.json")
shutil.copytree(OG_DIR + "database", OUTPUT_DIR + "database")
