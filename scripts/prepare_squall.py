# Author: Anuj Chauhan

import re
import os
import json
import shutil
import logging
import sqlite3
from tqdm import tqdm
from schema_generator import dump_db_json_schema

logging.basicConfig(level=logging.INFO)

squall_version = "squall"
OUTPUT_SQUALL_BASE_DIR = f"unified/{squall_version}"
os.makedirs(OUTPUT_SQUALL_BASE_DIR, exist_ok=True)

OUTPUT_DEV_FILE = f"{OUTPUT_SQUALL_BASE_DIR}/dev.jsonl"
OUTPUT_TRAIN_FILE = f"{OUTPUT_SQUALL_BASE_DIR}/train.jsonl"
OUTPUT_TABLES_FILE = f"{OUTPUT_SQUALL_BASE_DIR}/tables.json"

OG_SQUALL_BASE = "original/squall"
DB_JSON_DIR = f"{OG_SQUALL_BASE}/tables/json/"
OG_DATABSE_DIR = f"{OG_SQUALL_BASE}/tables/db/"
OG_TABLES_DIR = f"{OG_SQUALL_BASE}/tables/json/"

SQUALL_USED_DERIVED_COLUMNS_FILE = (
    "scripts/squall_table_to_used_derived_columns"
)
table_to_used_cols = json.load(open(SQUALL_USED_DERIVED_COLUMNS_FILE))

tables_used = set()
for entry in json.load(open(f"{OG_SQUALL_BASE}/data/squall.json", "r")):
    tables_used.add(entry["tbl"])
dbs = sorted(list(tables_used))

for path in [OUTPUT_DEV_FILE, OUTPUT_TRAIN_FILE, OUTPUT_TABLES_FILE]:
    if os.path.exists(path):
        os.unlink(path)

UNIFIED_DATABASE_DIR = f"unified/{squall_version}/database/"
if os.path.exists(UNIFIED_DATABASE_DIR):
    shutil.rmtree(UNIFIED_DATABASE_DIR)
os.makedirs(UNIFIED_DATABASE_DIR, exist_ok=True)

c_mapper_for_db = {}
for db in tqdm(dbs):
    db_id = db.split(".")[0]

    os.makedirs(os.path.join(UNIFIED_DATABASE_DIR, db_id), exist_ok=True)

    # Update the db content
    json_db = json.load(open(OG_TABLES_DIR + db_id + ".json"))

    headers = json_db["headers"][2:]
    max_c_x = len(headers)

    # SQLite column names cannot contain anything
    # except alpha numerics and underlines.
    headers = [
        header.replace("\n", "_")
        .replace("\t", "_")
        .replace(" ", "_")
        .replace("!", "")
        .replace("*", "")
        .replace("®", "")
        .replace("-", "_")
        .replace("<", "less_than")
        .replace("?", "_question_")
        .replace("(s)", "s")
        .replace("(", "")
        .replace(")", "")
        .replace("$", "_dollar_")
        .replace(".", "_")
        .replace("/", "_")
        .replace(",", "_")
        .replace("%", "_percent_")
        .replace("'", "")
        .replace("[", "")
        .replace("]", "")
        .replace(":", "")
        .replace("=", "_equal_")
        .replace("&", "_and_")
        .replace("{", "")
        .replace("}", "")
        .replace("+", "_plus_")
        .replace("#", "number")
        for header in headers
    ]

    sql_tokens = [
        keyword.strip().lower()
        for keyword in open("scripts/sqlite3_keywords.txt")
    ]

    # Default column names in squall
    sql_tokens.extend(["id", "agg"])
    headers = [
        "c_" + header
        if header in sql_tokens or not header[:1].isalpha()
        else header
        for header in headers
    ]  # 204_681
    headers = [re.sub("_+", "_", header) for header in headers]

    occurance_map = {
        2: "second_",
        3: "third_",
        4: "fourth_",
        5: "fifth_",
        6: "sixth_",
        7: "seventh_",
        8: "eighth_",
        9: "ninth_",
        10: "tenth_",
        11: "eleventh_",
    }

    # 200_24, 202_176 duplicate header
    seen_this_header_count = {}
    for idx, header in enumerate(headers):
        if header in seen_this_header_count:
            headers[idx] = (
                occurance_map[seen_this_header_count[header] + 1] + header
            )
            seen_this_header_count[header] += 1
        else:
            seen_this_header_count[header] = 1

    c_mapping = {}
    for i in range(1, len(headers) + 1):
        c_mapping[f"c{i}"] = headers[i - 1]

    c_mapper_for_db[db_id] = c_mapping

    original_sql_path = f"{UNIFIED_DATABASE_DIR}/{db_id}/original_{db_id}.sql"
    original_db = f"{OG_DATABSE_DIR}/{db_id}.db"

    con = sqlite3.connect(original_db)
    sql_lines = []
    for line in con.iterdump():
        sql_lines.append(line)
        open(original_sql_path, "a").write(line + "\n")
    con.close()

    unified_sql_path = f"{UNIFIED_DATABASE_DIR}/{db_id}/{db_id}.sql"
    unified_db_path = f"{UNIFIED_DATABASE_DIR}/{db_id}/{db_id}.sqlite"

    FOREIGN_KEY_INJECTION = ", FOREIGN KEY(m_id) references w(id)"

    for line in sql_lines:
        new_line = ""
        for token in line.split():
            if token.startswith("t_") or token.startswith('"t_'):
                token = token.replace("t_", "", 1)
            match = re.search(r"[c]\d+", token)
            if match:
                c_token = match.group()
                # 202_112 has c1968 as cell value.
                # 204_850 has c0001 as cell value.
                if (
                    0 < int(c_token[1:]) <= max_c_x
                    and "VALUES(" not in token
                    and len(c_token) < 4
                ):
                    token = token.replace(c_token, c_mapping[c_token])

            new_line = f"{new_line} {token}" if new_line else f"{token}"

        if new_line.startswith("CREATE TABLE"):
            if not new_line.startswith("CREATE TABLE w"):
                idx = new_line.index(");")
                new_line = (
                    new_line[:idx] + FOREIGN_KEY_INJECTION + new_line[idx:]
                )

        with open(unified_sql_path, "a") as f:
            f.write(new_line + "\n")

    # Creating modified sqlite databases
    conn = sqlite3.connect(unified_db_path)
    conn.executescript(open(unified_sql_path).read())

print("Creating tables.json")
tables = []
for db in tqdm(dbs):
    db_id = db.split(".")[0]
    unified_db_path = f"{UNIFIED_DATABASE_DIR}/{db_id}/{db_id}.sqlite"
    tables.append(dump_db_json_schema(unified_db_path))

with open(OUTPUT_TABLES_FILE, "w") as f:
    json.dump(tables, f, indent=2)

# Create train/dev files.
print("Writing train/dev files.")
dev_ids = []
dev_set = []
train_set = []

for i in range(5):
    with open("original/squall/data/dev-{}.ids".format(i)) as f:
        dev_ids.append(set(json.load(f)))

with open("original/squall/data/squall.json") as f:
    squall_data = json.load(f)

for i in range(5):
    dev_set = [x for x in squall_data if x["tbl"] in dev_ids[i]]
    train_set = [x for x in squall_data if x["tbl"] not in dev_ids[i]]


def append_join_clause(entry_sql, db_id, mapper):
    original_sql = " ".join(token[1] for token in entry_sql)
    mentioned_columns = set([x[1] for x in entry_sql if x[0] == "Column"])

    db_path = f"{OG_DATABSE_DIR}/{db_id}.db"
    # db_path = f"{UNIFIED_DATABASE_DIR}/{db_id}/{db_id}.sqlite"
    cursor = sqlite3.connect(db_path).cursor()

    cursor.execute("SELECT * FROM sqlite_master WHERE type='table'")

    all_tables = cursor.fetchall()

    join_tables = []
    for t in all_tables:
        if t[1] != "w":
            cursor.execute(f"PRAGMA table_info({t[1]})")
            all_columns = cursor.fetchall()
            column_names = set([x[1] for x in all_columns])
            if mentioned_columns.intersection(column_names):
                join_tables.append(t[1])

    if join_tables:
        join_tables = [jt[2:] for jt in join_tables if jt.startswith("t_")]
        segments = re.split("from w", original_sql)
        from_string = "from w"
        for jt in join_tables:
            from_string += f" JOIN {jt} ON w.id = {jt}.m_id"
        original_sql = from_string.join(segments)

    return original_sql


for entry in dev_set:
    mapper = c_mapper_for_db[entry["tbl"]]

    unified_format_entry = {}
    unified_format_entry["db_id"] = entry["tbl"]
    unified_format_entry["question"] = " ".join(token for token in entry["nl"])
    sql_with_join = append_join_clause(entry["sql"], entry["tbl"], mapper)
    unified_format_entry["query"] = " ".join(
        mapper[token.split("_")[0]] + token[len(token.split("_")[0]) :]
        if token.split("_")[0] in mapper
        else token
        for token in sql_with_join.split()
    )
    unified_format_entry["target"] = entry["tgt"]

    with open(OUTPUT_DEV_FILE, "a+") as f:
        json.dump(unified_format_entry, f)
        f.write("\n")

for entry in train_set:
    mapper = c_mapper_for_db[entry["tbl"]]

    unified_format_entry = {}
    unified_format_entry["db_id"] = entry["tbl"]
    unified_format_entry["question"] = " ".join(token for token in entry["nl"])
    sql_with_join = append_join_clause(entry["sql"], entry["tbl"], mapper)
    unified_format_entry["query"] = " ".join(
        mapper[token.split("_")[0]] + token[len(token.split("_")[0]) :]
        if token.split("_")[0] in mapper
        else token
        for token in sql_with_join.split()
    )

    unified_format_entry["target"] = entry["tgt"]

    with open(OUTPUT_TRAIN_FILE, "a+") as f:
        json.dump(unified_format_entry, f)
        f.write("\n")
