# Author: Anuj Chauhan

import os
import json
import shutil

# Uses the same tables.json and database/ as og spider.
FILES_TO_CONVERT = ["train_spider", "dev"]

INPUT_DIR = "original/Spider-Syn/Spider-Syn/"
OUTPUT_DIR = "unified/spider_syn/"
SPIDER_DIR = "unified/spider/"

COPY_FROM_SPIDER = ["database/", "tables.json"]
for entry in COPY_FROM_SPIDER:
    if entry.endswith("/"):
        shutil.copytree(
            SPIDER_DIR + entry, OUTPUT_DIR + entry, dirs_exist_ok=True
        )
    else:
        shutil.copy(SPIDER_DIR + entry, OUTPUT_DIR + entry)

# Cleaning output files.
for f_name in FILES_TO_CONVERT:
    output_file = OUTPUT_DIR + f_name + ".jsonl"
    if os.path.exists(output_file):
        os.unlink(output_file)

    with open(INPUT_DIR + f_name + ".json") as input_file_ptr:
        json_entries = json.load(input_file_ptr)
        for entry in json_entries:
            unified_json_entry = {}
            unified_json_entry["db_id"] = entry["db_id"]
            unified_json_entry["question"] = entry["SpiderSynQuestion"]
            unified_json_entry["query"] = entry["query"]

            with open(OUTPUT_DIR + f_name + ".jsonl", "a+") as writer:
                json.dump(unified_json_entry, writer)
                writer.write("\n")
