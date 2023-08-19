# Author: Anuj Chauhan

import os
import json
import numpy
from tqdm import tqdm
from pandas import json_normalize
from collections import defaultdict
from time import time, strftime, gmtime
from sqlglot import parse_one, exp, transpile

STATS_OUTPUT_DIR = "stats/output"
STATS_ERRORS_DIR = f"stats/parsing_errors"

os.makedirs(STATS_OUTPUT_DIR, exist_ok=True)
os.makedirs(STATS_ERRORS_DIR, exist_ok=True)

UNIFIED_DIR = "unified"
ALL_DATABASES = os.listdir(UNIFIED_DIR)


def get_nlq_stats(jsonl_content, dataset):
    nlq_level_join_count = []
    nlq_level_select_count = []
    nlq_level_nest_level = []
    sqlglot_parsing_errors = 0
    count = 0

    for line in jsonl_content:
        query = json.loads(line)["query"]

        try:
            transpile(query)
        except Exception as e:
            error_file = f"{STATS_ERRORS_DIR}/{dataset}"
            if os.path.exists(error_file):
                os.unlink(error_file)

            with open(error_file, "a+") as writer:
                writer.write(f"{query} \n {str(e)} \n\n")

            sqlglot_parsing_errors += 1
            continue

        count += 1
        parsed_query = parse_one(query)

        join_count = sum(1 for _ in parsed_query.find_all(exp.Join))
        select_count = sum(1 for _ in parsed_query.find_all(exp.Select))
        intersect_count = sum(1 for _ in parsed_query.find_all(exp.Intersect))
        except_count = sum(1 for _ in parsed_query.find_all(exp.Except))

        nest_level = select_count - (intersect_count + except_count)

        nlq_level_join_count.append(join_count)
        nlq_level_select_count.append(select_count)
        nlq_level_nest_level.append(nest_level)

    return {
        "total_nlqs": count,
        "parsing_errors": sqlglot_parsing_errors,
        "join_counts": round(sum(nlq_level_join_count) / count, 2),
        "select_counts": round(sum(nlq_level_select_count) / count, 2),
        "nest_levels": round(sum(nlq_level_nest_level) / count, 2),
    }


def get_schema_stats(schema):
    db_count = 0
    tables_count = 0
    columns_count = 0

    primary_key_count = 0
    foreign_key_count = 0

    schema = json.loads(schema.read())
    db_count = len(schema)

    for db in schema:
        tables_count += len(db["table_names_original"])
        columns_count += len(db["column_names_original"])
        primary_key_count += len(db["primary_keys"])
        foreign_key_count += len(db["foreign_keys"])

    return {
        "db_count": db_count,
        "avg_tables_per_db": round(tables_count / db_count, 2),
        "avg_columns_per_table": round(columns_count / tables_count, 2),
        "avg_pk_per_table": round(primary_key_count / tables_count, 2),
        "avg_fk_per_table": round(foreign_key_count / tables_count, 2),
    }


def normalizer(node):
    if isinstance(node, exp.Column):
        return parse_one("PLACEHOLDER_COLUMN")
    elif isinstance(node, exp.Table):
        return parse_one("PLACEHOLDER_TABLE")
    elif isinstance(node, exp.Literal):
        return parse_one("PLACEHOLDER_LITERAL")
    elif type(node) in [
        exp.Count,
        exp.Sum,
        exp.Min,
        exp.Max,
        exp.Avg,
        exp.Quantile,
        exp.Stddev,
        exp.StddevPop,
        exp.StddevSamp,
    ]:
        return parse_one("PLACEHOLDER_AGG")
    elif type(node) in [exp.LT, exp.LTE, exp.GT, exp.GTE]:
        return parse_one("PLACEHOLDER_COMPARISON")
    return node


def noramlize_sql(parsed_sql):
    return parsed_sql.transform(normalizer).sql()


def get_redundancy_stats(jsonl_content):
    count = 0
    distinct_sql_patterns = defaultdict(list)

    for line in jsonl_content:
        query = json.loads(line)["query"]

        try:
            transpile(query)
        except Exception as e:
            print(f"Could not transpile: {query}\n{e}")
            continue

        count += 1
        parsed_query = parse_one(query)
        distinct_sql_patterns[noramlize_sql(parsed_query)].append(query)
        max_queries_per_pattern = max(len(queries) for queries in distinct_sql_patterns.values())
        standard_deviation_of_queries_per_pattern = numpy.std(
            [len(queries) for queries in distinct_sql_patterns.values()]
        )

    return {
        "nlqs": count,
        "unique_sql_patterns": len(distinct_sql_patterns),
        "total_nlqs_by_unique_patterns": round(count / len(distinct_sql_patterns), 2),
        "max_queries_per_pattern": max_queries_per_pattern,
        "std_dev_queries_per_pattern": round(standard_deviation_of_queries_per_pattern, 2),
    }


def collect_dataset_level_statistics():
    all_schema_stats = []
    all_nlq_stats = []
    all_redundancy_stats = []

    # Schema and NLQ statistics
    for dataset in tqdm(ALL_DATABASES, position=0, leave=True):
        print(f"Getting stats for {dataset}")
        DATASET_CONTENT_DIR = f"{UNIFIED_DIR}/{dataset}"

        db_nlq_stats = {}
        db_nlq_stats["db_id"] = dataset

        db_redundancy_stats = {}
        db_redundancy_stats["db_id"] = dataset

        for content in os.listdir(DATASET_CONTENT_DIR):
            if content == "tables.json":
                db_schema_stats = {}
                db_schema_stats["db_id"] = dataset
                db_schema_stats.update(get_schema_stats(open(f"{DATASET_CONTENT_DIR}/{content}")))
                all_schema_stats.append(db_schema_stats)

            if content.endswith(".jsonl"):
                split = content.split(".jsonl")[0]

                db_nlq_stats[split] = get_nlq_stats(
                    open(f"{DATASET_CONTENT_DIR}/{content}"), dataset
                )
                db_redundancy_stats[split] = get_redundancy_stats(
                    open(f"{DATASET_CONTENT_DIR}/{content}")
                )

        all_nlq_stats.append(db_nlq_stats)
        all_redundancy_stats.append(db_redundancy_stats)

    schema_stats_df = json_normalize(all_schema_stats)
    nlq_stats_df = json_normalize(all_nlq_stats)
    redundancy_stats_df = json_normalize(all_redundancy_stats)

    for df in [schema_stats_df, nlq_stats_df, redundancy_stats_df]:
        df.fillna("-", inplace=True)

    schema_stats_df.to_csv(f"{STATS_OUTPUT_DIR}/schema_stats.csv")
    nlq_stats_df.to_csv(f"{STATS_OUTPUT_DIR}/nlq_stats.csv")
    redundancy_stats_df.to_csv(f"{STATS_OUTPUT_DIR}/redundancy_stats.csv")


def collect_unified_statisitcs():
    unified_stats = defaultdict(int)

    all_nlq_count = 0
    jsonl_files = ["test.jsonl", "train.jsonl", "dev.jsonl"]
    for dataset in tqdm(ALL_DATABASES):
        DATASET_CONTENT_DIR = f"{UNIFIED_DIR}/{dataset}"

        for content in os.listdir(DATASET_CONTENT_DIR):
            if content in jsonl_files:
                with open(f"{DATASET_CONTENT_DIR}/{content}", "r") as reader:
                    for line in reader.readlines():
                        line = json.loads(line)

                        try:
                            all_nlq_count += 1
                            parsed_query = parse_one(line["query"])

                            normalized_sql = noramlize_sql(parsed_query)
                            normalized_sql = normalized_sql.replace(
                                "PLACEHOLDER_COLUMN = PLACEHOLDER_COLUMN",
                                "PLACEHOLDER_COLUMN = PLACEHOLDER_LITERAL",
                            )
                            # Revert back for JOIN clause equalities.
                            normalized_sql = normalized_sql.replace(
                                "ON PLACEHOLDER_COLUMN = PLACEHOLDER_LITERAL",
                                "PLACEHOLDER_COLUMN = PLACEHOLDER_COLUMN",
                            )

                            unified_stats[normalized_sql] += 1
                        except Exception as e:
                            print(f"Exception transforming: {parsed_query}: {e}")
                            continue

    unified_stats = dict(sorted(unified_stats.items(), key=lambda item: -item[1]))

    json.dump(
        unified_stats,
        open(f"{STATS_OUTPUT_DIR}/unified_sql_patterns.json", "w"),
        indent=4,
    )


if __name__ == "__main__":
    start = time()
    collect_dataset_level_statistics()
    collect_unified_statisitcs()
    print(f"Took {strftime('%Mm%Ss', gmtime(time() - start))}")
