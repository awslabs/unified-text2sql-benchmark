# Author: Anuj Chauhan

# https://www.sqlite.org/lang_altertable.html
# SQLite is not fully compatible with CONSTRAINT so need to modify og .sql file
# Also the database content is given in CSV which needs to be added separately.
# But these csv are missing headers so need to map column names from
# table names and inject them at the top.

import os
import json
import shutil
import sqlite3
import pandas as pd
from schema_generator import dump_db_json_schema


OG_DIR = "original/fiben-benchmark/"
UNIFIED_BASE_DIR = "unified/fiben/"

# Start fresh.
if os.path.exists(UNIFIED_BASE_DIR):
    shutil.rmtree(UNIFIED_BASE_DIR)

os.makedirs(UNIFIED_BASE_DIR)

OG_SQL_FILE = os.path.join(OG_DIR, "FIBEN.sql")
SQLITE_COMPATIBLE_SQL_FILE = os.path.join(
    UNIFIED_BASE_DIR + "sqlite_fiben.sql"
)


def _create_sqlite_compatible_sql_file():
    all_lines = open(OG_SQL_FILE).readlines()

    table_name_to_alter_line = {}

    for line in all_lines:
        if line.startswith("ALTER"):
            table_name = line.split("ALTER TABLE ")[1].split()[0]
            FK_info = line[line.index("FOREIGN KEY ") : -2]
            table_name_to_alter_line[table_name] = FK_info

    for line in all_lines:
        if line.startswith("CREATE"):
            table_name = line.split("CREATE TABLE ")[1].split()[0]
            FK_info = table_name_to_alter_line.get(table_name)

            if FK_info:
                line = line[:-3] + f" {FK_info}" + line[-3:]
            with open(SQLITE_COMPATIBLE_SQL_FILE, "a") as writer:
                writer.write(line)


_create_sqlite_compatible_sql_file()

UNIFIED_DB_PATH = os.path.join(UNIFIED_BASE_DIR, "database", "fiben")
os.makedirs(UNIFIED_DB_PATH)
SQLITE_FILE = os.path.join(UNIFIED_DB_PATH, "fiben.sqlite")

conn = sqlite3.connect(SQLITE_FILE)
conn.executescript(open(SQLITE_COMPATIBLE_SQL_FILE).read())

tables = []
tables.append(dump_db_json_schema(SQLITE_FILE))
json.dump(
    tables, open(os.path.join(UNIFIED_BASE_DIR, "tables.json"), "w"), indent=2
)

table_name_to_column_names = {}
for table_idx, table_name in enumerate(tables[0]["table_names_original"]):
    for idx, column_name in tables[0]["column_names_original"]:
        if idx == table_idx:
            if table_name not in table_name_to_column_names:
                table_name_to_column_names[table_name] = []

            table_name_to_column_names[table_name].append(column_name)

CELL_VALUE_DIR = os.path.join(OG_DIR, "data")
cell_value_files = os.listdir(CELL_VALUE_DIR)

for file_name in cell_value_files:
    table_name = file_name.split(".csv")[0]
    assert (
        table_name in table_name_to_column_names
    ), f"Out of schema cell value file found: {file_name}"

    cell_value_df = pd.read_csv(
        os.path.join(CELL_VALUE_DIR, file_name),
        names=table_name_to_column_names[table_name],
        header=None,
    )
    cell_value_df.to_sql(table_name, conn, if_exists="append", index=False)

# Finally create the dev file
input_data = json.load(open(os.path.join(OG_DIR, "FIBEN_Queries.json")))

with open(os.path.join(UNIFIED_BASE_DIR, "dev.jsonl"), "a") as writer:
    for entry in input_data:
        unified_json_entry = {}
        unified_json_entry["db_id"] = "fiben"
        unified_json_entry["question"] = entry["question"]

        clean_query = entry["SQL"].replace("FIBEN.", "")

        # sqlite uses LIMIT instead of FETCH clause.
        if "FETCH" in clean_query:
            limit_num = (
                clean_query.split("FETCH FIRST")[1]
                .split("ROWS ONLY")[0]
                .strip()
            )
            idx = clean_query.index("FETCH")
            clean_query = clean_query[:idx] + f"LIMIT {limit_num}"

        unified_json_entry["query"] = clean_query

        json.dump(unified_json_entry, writer)
        writer.write("\n")
