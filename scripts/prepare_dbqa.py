# Author: Anuj Chauhan

import os
import json
import shutil

OG_DIR = "original/kaggle-dbqa/"
OUTPUT_DIR = "unified/dbqa/"
OUTPUT_DEV_FILE = os.path.join(OUTPUT_DIR, "dev.jsonl")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Using all the sqls for validation only.
INPUT_DATA_DIR = os.path.join(OG_DIR, "examples")

EXAMPLE_FILES = os.listdir(INPUT_DATA_DIR)
EXAMPLE_FILES = [
    file
    for file in EXAMPLE_FILES
    if file.endswith(".json") and not ("_test" in file or "_fewshot" in file)
]

for file in EXAMPLE_FILES:
    input_data = json.load(open(os.path.join(INPUT_DATA_DIR, file)))

    for entry in input_data:
        unified_json_entry = {}
        unified_json_entry["db_id"] = entry["db_id"]
        unified_json_entry["query"] = entry["query"]
        unified_json_entry["question"] = entry["question"]

        with open(OUTPUT_DEV_FILE, "a+") as writer:
            json.dump(unified_json_entry, writer)
            writer.write("\n")

shutil.copy(OG_DIR + "KaggleDBQA_tables.json", OUTPUT_DIR + "tables.json")
shutil.copytree(OG_DIR + "databases", OUTPUT_DIR + "database")
