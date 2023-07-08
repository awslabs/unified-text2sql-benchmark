# Author: Anuj Chauhan

# Source input from `sql_state_tracking`
# history_text is a ordered list of previous questions in the same interaction.

import os
import json
import shutil
from shutil import rmtree
from schema_generator import dump_db_json_schema

OG_DIR = "original/cosql_dataset/"
OUTPUT_DIR = "unified/cosql/"

# Fresh start
rmtree(OUTPUT_DIR, ignore_errors=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

INPUT_DATA_DIR = os.path.join(OG_DIR, "sql_state_tracking")
INPUT_FILES = os.listdir(INPUT_DATA_DIR)

INPUT_DEV_FILE = [file for file in INPUT_FILES if file.endswith("_dev.json")][
    0
]
INPUT_TRAIN_FILE = [
    file for file in INPUT_FILES if file.endswith("_train.json")
][0]

FILES_TO_CONVERT = [INPUT_DEV_FILE, INPUT_TRAIN_FILE]

for file in FILES_TO_CONVERT:
    OUTPUT_FILE = (
        "unified/cosql/dev.jsonl"
        if "dev" in file
        else "unified/cosql/train.jsonl"
    )

    input_data = json.load(open(os.path.join(INPUT_DATA_DIR, file)))

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

shutil.copytree(
    OG_DIR + "database", OUTPUT_DIR + "database", dirs_exist_ok=True
)

# Re-creating since in the original dataset there is databse (travel_agent)
# in the tables.json which does not exist in the database folder.

tables = []
cosql_databases = os.listdir(OUTPUT_DIR + "database")

for db_name in cosql_databases:
    sqlite_file = os.path.join(
        OUTPUT_DIR, "database", db_name, f"{db_name}.sqlite"
    )
    tables.append(dump_db_json_schema(sqlite_file))

json.dump(tables, open(os.path.join(OUTPUT_DIR, "tables.json"), "w"), indent=2)
