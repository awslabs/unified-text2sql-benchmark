# Author: Anuj Chauhan

import os
import json

FILES_TO_CONVERT = ["train_spider", "train_others", "dev", "tables"]

INPUT_DIR = "../original/spider/"
OUTPUT_DIR = "../unified/spider/"

SELECTED_TRAINING_DATA_KEYS = ["db_id", "query", "question"]
SELECTED_TABLES_KEYS = [
    "db_id",
    "table_names",
    "column_names",
    "column_types",
    "primary_keys",
    "foreign_keys",
]

# Cleaning output files.
for f_name in FILES_TO_CONVERT:
    output_file = OUTPUT_DIR + f_name + ".jsonl"
    if os.path.exists(output_file):
        os.unlink(output_file)

for f_name in FILES_TO_CONVERT:
    with open(INPUT_DIR + f_name + ".json") as input_file_ptr:
        json_entries = json.load(input_file_ptr)
        for entry in json_entries:
            selected_json_entry = {}
            for key in (
                SELECTED_TABLES_KEYS
                if "tables" in f_name
                else SELECTED_TRAINING_DATA_KEYS
            ):
                selected_json_entry[key] = entry[key]

            with open(OUTPUT_DIR + f_name + ".jsonl", "a+") as writer:
                json.dump(selected_json_entry, writer)
                writer.write("\n")
