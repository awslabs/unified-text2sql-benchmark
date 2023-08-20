# Author: Jun Wang

import os
import json
import shutil

from schema_generator import dump_db_json_schema


def extract_nlq_sql_pair(input_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    filename = "test.jsonl"

    output_file_path = os.path.join(output_dir, filename)
    csv_reader = open(input_file, "rt")

    # Column names
    _ = next(csv_reader)

    outputs = []
    for i, l in enumerate(csv_reader):
        l = l.strip().split(";")
        if len(l) == 2:
            line = dict(query=l[1], question=l[0], db_id="seoss_data")
        else:
            line = dict(id=i, query=l[1], question=l[0], domain=l[2], db_id="seoss_data")
        outputs.append(json.dumps(line))
    with open(output_file_path, "w") as fout:
        fout.write("\n".join(outputs))


if __name__ == "__main__":
    original_db_path = "original/SEOSS-Queries/dataset/"
    queries_dir = f"{original_db_path}/SEOSS_Queries_orchestrated"
    queries_csv = f"{queries_dir}/seoss_queries_orchestrated.csv"

    output_dir = "unified/seoss"
    extract_nlq_sql_pair(queries_csv, output_dir)

    output_db_dir = f"{output_dir}/database/seoss_data"
    os.makedirs(output_db_dir, exist_ok=True)

    output_db_file = f"{output_db_dir}/seoss_data.sqlite"

    shutil.copy(
        f"{queries_dir}/Database and DB schema/apache-pig/apache-pig.sqlite", output_db_file
    )

    tables = []
    tables.append(dump_db_json_schema(output_db_file))
    json.dump(tables, open(os.path.join(output_dir, "tables.json"), "w"), indent=2)

    print(f"Finished.")
