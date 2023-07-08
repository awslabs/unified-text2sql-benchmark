# Author: Anuj Chauhan

import os
import json
import shutil

OG_DIR = "original/Spider-DK/"
OUTPUT_DIR = "unified/spider_dk/"
OUTPUT_TEST_FILE = os.path.join(OUTPUT_DIR, "test.jsonl")

os.makedirs(OUTPUT_DIR, exist_ok=True)
input_data = json.load(open(os.path.join(OG_DIR, "Spider-DK.json")))

for entry in input_data:
    unified_json_entry = {}
    unified_json_entry["db_id"] = entry["db_id"]
    unified_json_entry["query"] = entry["query"]
    unified_json_entry["question"] = entry["question"]

    with open(OUTPUT_TEST_FILE, "a+") as writer:
        json.dump(unified_json_entry, writer)
        writer.write("\n")

shutil.copy(OG_DIR + "tables.json", OUTPUT_DIR + "tables.json")
shutil.copytree(
    OG_DIR + "database", OUTPUT_DIR + "database", dirs_exist_ok=True
)

# Also copy over all spider databases
SPIDER_DB_DIR = "original/spider/database/"
databases = os.listdir(SPIDER_DB_DIR)

for db in databases:
    shutil.copytree(
        SPIDER_DB_DIR + db, OUTPUT_DIR + f"database/{db}", dirs_exist_ok=True
    )
