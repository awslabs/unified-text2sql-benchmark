# Compiled by: Joe Lilien

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import os
import shutil
import time
import numpy as np
import sqlite3
import timeout_decorator
import pathlib
import tensorflow.compat.v1.gfile as gfile
import csv
import sqlparse
from tqdm import tqdm


from scripts.schema_generator import dump_db_json_schema

########################################################################################################################
# Before running this script, make sure to go to https://github.com/google-research/language/tree/master/language/xsp
# and follow the instructions for the following steps:
#  * (1) Downloading resources [For evaluation] -> `sh language/xsp/data_download.sh`
#  * (2) Copy the following resources into a folder with this structure for each run (i.e. 'atis'): `original/{run}/`
#         * {run}.sqlite (same as {run}.db file, just rename.. must include cell values)
#           * For `imdb` the file is in MySql, so it must be converted. I used https://github.com/dumblob/mysql2sqlite
#         * {run}_empty.sqlite (empty .db file created in step 1, rename suffix)
#         * {run}.json file with sql/nlq examples
#         * {run}_schema.csv with table schemas
#
# For each subcategory listed under RUNS below, make sure to have a folder in the working directory called
# ./original/{run} (i.e. original/atis) with the following (downloaded/created in steps above):
#   * {run}.sqlite
#   * {run}.json
#   * {run}_schema.csv
########################################################################################################################


RUNS = [
    "atis",
    "geoquery",
    "scholar",
    "advising",
    "restaurants",
    "academic",
    "imdb",
    "yelp",
]


def main():
    for idx, run in enumerate(RUNS):
        print(f"({idx + 1}/{len(RUNS)}) Starting run for {run}:")
        input_dir = f"./original/{run}"
        mid_dir = f"./intermediate/{run}"
        output_dir = f"./unified/{run}"

        if run in ["atis"]:
            splits = ["dev"]
        elif run in ["geoquery", "scholar", "advising"]:
            splits = ["train", "dev"]
        elif run in ["restaurants", "academic", "imdb", "yelp"]:
            splits = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        else:
            raise Exception(f"Unexpected run name: {run}")

        pathlib.Path(input_dir).mkdir(parents=True, exist_ok=True)
        pathlib.Path(mid_dir).mkdir(parents=True, exist_ok=True)
        pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

        # HACK: Fix queries with missing 'sql-only' variables by replacing variable name with "" in SQL statement
        print("Fixing queries with missing 'sql-only' variables: ")
        with open(f"{input_dir}/{run}.json") as q_in:
            data = json.load(q_in)
            for q_set in tqdm(data):
                sql_vars = [
                    v
                    for v in q_set["variables"]
                    if v["location"] == "sql-only"
                ]
                for ex in q_set["sentences"]:
                    for sql_var in sql_vars:
                        ex["variables"][sql_var["name"]] = ""
        fixed_data_path = f"{mid_dir}/{run}_fixed.json"
        with open(fixed_data_path, "w") as q_out:
            json.dump(data, q_out, indent=4)

        db_path = f"{input_dir}/{run}.sqlite"
        # For the 'scholar' dataset, we want to reduce database size so that we can operate properly on the DB
        if run == "scholar":
            percent_of_data_to_keep = 0.01
            print(
                f"Reducing {run} database size to {percent_of_data_to_keep} of original"
            )
            reduce_database_size(
                database_fp=db_path, percent_to_keep=percent_of_data_to_keep
            )

        # Create tables.json
        tables = dump_db_json_schema(db=db_path)
        print(f"Reading {db_path} file and writing out to tables.json.")
        with open(f"{output_dir}/tables.json", "w+") as f:
            json.dump([tables], f, indent=2)

        cache_filepath = f"{mid_dir}/cache.json"
        # Create cache for db:
        print(f"Creating cache file for db {run}.")
        create_cache(
            dataset_name=run,
            fixed_data_path=fixed_data_path,
            cache_path=cache_filepath,
            errors_filepath=f"{mid_dir}/cache_exec_errors.txt",
            splits=splits,
        )

        formatted_preds_filename = f"{run}_predictions.json"
        print(
            f"Storing output gt predictions file to {formatted_preds_filename}"
        )
        # Create formatted predictions file with GT sql queries
        gen_predictions(
            run=run,
            fixed_data_path=fixed_data_path,
            splits=splits,
            input_dir=input_dir,
            mid_dir=mid_dir,
            output_filename=formatted_preds_filename,
        )

        output_eval_filename = "dataset_predictions.txt"
        print(f"Running eval script, store results to {output_eval_filename}")
        # Run evaluation script to generate table outputs from gold queries
        run_evaluation(
            predictions_filepath=f"{mid_dir}/{formatted_preds_filename}",
            output_filepath=f"{mid_dir}/{output_eval_filename}",
            cache_filepath=cache_filepath,
            verbose=False,
            update_cache=False,
        )

        # Get list of nlqs to filter out of final set
        nlqs_to_remove = get_nlqs_to_remove(
            eval_filename=f"{mid_dir}/{output_eval_filename}"
        )
        print(
            f"After filtering, discovered {len(nlqs_to_remove)} nlqs to remove."
        )

        print("Writing cache with anonymized_alias queries to output dir")
        with open(cache_filepath) as c:
            cache_obj = json.load(c)
        anon_cache_obj = {}
        for query, result in tqdm(cache_obj.items()):
            if isinstance(result, str):
                anon_cache_obj[query] = preprocess_sql(result)
            elif isinstance(result, list):
                anon_cache_obj[preprocess_sql(query)] = result
            else:
                raise Exception(query, result)
        with open(f"{output_dir}/cache.json", "w+") as c_out:
            json.dump(anon_cache_obj, c_out)

        out_lines = []
        removed_nlqs = {}
        count = 0
        for split in splits:
            print(f"Saving query data for {split} split.")
            with open(fixed_data_path) as q:
                queries_obj = json.load(q)

            # The UMichigan data is split by anonymized queries, where values are
            # anonymized but table/column names are not. However, our experiments are
            # performed on the original splits of the data.
            # count = 0
            for q_set in tqdm(queries_obj):
                # Take the first SQL query only. From their Github documentation:
                # "Note - we only use the first query, but retain the variants for
                #  completeness"
                # See https://github.com/google-research/language/blob/master/language/xsp/data_preprocessing/michigan_preprocessing.py#L44
                sql_txt = q_set["sql"][0]
                for example in q_set["sentences"]:
                    if example["question-split"] not in split:
                        continue
                    question = example["text"]
                    query = sql_txt

                    # Go through the anonymized values and replace them in both the natural
                    # language and the SQL.
                    #
                    # It's very important to sort these in descending order. If one is a
                    # substring of the other, it shouldn't be replaced first lest it ruin the
                    # replacement of the superstring.
                    for v_name, v_value in sorted(
                        example["variables"].items(),
                        key=lambda x: len(x[0]),
                        reverse=True,
                    ):
                        if not v_value:
                            # From https://github.com/google-research/language/blob/master/language/xsp/data_preprocessing/michigan_preprocessing.py#L67:
                            # TODO(alanesuhr) While the Michigan repo says to use a - here, the
                            # thing that works is using a % and replacing = with LIKE.
                            #
                            # It's possible that I should remove such clauses from the SQL, as
                            # long as they lead to the same table result. They don't align well
                            # to the natural language at least.
                            #
                            # See: https://github.com/jkkummerfeld/text2sql-data/tree/master/data
                            v_value = "%"
                        question = question.replace(v_name, v_value)
                        query = query.replace(v_name, v_value)

                    # In the case that we replaced an empty anonymized value with %, make it
                    # compilable new allowing equality with any string.
                    query = query.replace('= "%"', 'LIKE "%"')
                    query = query.replace("= %", 'LIKE "%"')
                    query = preprocess_sql(query)

                    if question in nlqs_to_remove:
                        removed_nlqs[question] = 2
                        continue

                    sent_obj = {
                        "db_id": run,
                        "question": question,
                        "query": query,
                    }
                    count += 1
                    out_lines.append(sent_obj)

        print(f"  Writing out test.jsonl file for {run}: {count} examples")
        with open(f"{output_dir}/test.jsonl", "w+") as o:
            for ex in out_lines:
                json.dump(ex, o)
                o.write("\n")

        missed = []
        for nlq in nlqs_to_remove:
            if nlq not in removed_nlqs:
                missed.append(nlq)
        print(f"Missed removing {len(missed)} NLQs!")
        print(missed)


########################################################################################################################
# * WARNING: This script modifies the DB file which is loaded into the function! However, a copy of the original will be
# saved with the extension ".orig", assuming that the option is set.
########################################################################################################################
def reduce_database_size(
    database_fp: str, percent_to_keep: float, save_copy_of_orig: bool = True
):
    # Copy DB file to save the original copy
    if save_copy_of_orig:
        dst_fp = f"{database_fp}.orig"
        print(f"Saving copy of original {database_fp} to {dst_fp}")
        shutil.copy(src=database_fp, dst=dst_fp)
    con = sqlite3.connect(database_fp)
    cur = con.cursor()
    list_tables_query = (
        """SELECT name FROM sqlite_master WHERE type='table';"""
    )
    cur.execute(list_tables_query)
    tables = [t[0] for t in cur.fetchall()]
    get_num_rows_query_template = """SELECT COUNT(*) FROM {table}"""
    create_temp_table_query_template = """CREATE TEMPORARY TABLE temp_{table} as SELECT * FROM {table} LIMIT {num_rows};"""
    delete_from_table_query_template = """DELETE FROM {table};"""
    copy_from_temp_table_query = (
        """INSERT INTO {table} SELECT * FROM temp_{table};"""
    )
    vacuum_command = """VACUUM"""
    for table in tqdm(tables):
        cur.execute(get_num_rows_query_template.format(table=table))
        num_rows = int(cur.fetchone()[0])
        print(num_rows)
        num_rows_to_keep = int(num_rows * percent_to_keep)
        print(f"Reducing {table} from {num_rows} -> {num_rows_to_keep} rows")
        print(
            create_temp_table_query_template.format(
                num_rows=num_rows_to_keep, table=table
            )
        )
        cur.execute(
            create_temp_table_query_template.format(
                num_rows=num_rows_to_keep, table=table
            )
        )
        print(
            delete_from_table_query_template.format(
                num_rows=num_rows_to_keep, table=table
            )
        )
        cur.execute(
            delete_from_table_query_template.format(
                num_rows=num_rows_to_keep, table=table
            )
        )
        print(
            copy_from_temp_table_query.format(
                num_rows=num_rows_to_keep, table=table
            )
        )
        cur.execute(
            copy_from_temp_table_query.format(
                num_rows=num_rows_to_keep, table=table
            )
        )
        cur.execute(get_num_rows_query_template.format(table=f"temp_{table}"))
        num_rows = int(cur.fetchone()[0])
        print(num_rows)

    print("Vacuuming database and committing results")
    con.isolation_level = None
    con.execute(vacuum_command)
    con.isolation_level = ""
    con.commit()
    con.close()


########################################################################################################################
# Code adapted from https://github.com/google-research/language/blob/master/language/xsp/data_utils/create_cache.py
# """Creates a cache for the specified dataset by executing the gold queries."""
def create_cache(
    dataset_name, fixed_data_path, cache_path, errors_filepath, splits
):
    if dataset_name == "spider":
        pass
    else:
        db = sqlite3.connect(
            os.path.join(f"original/{dataset_name}", dataset_name + ".sqlite")
        )
        c = db.cursor()

        cache = dict()

        if os.path.exists(cache_path):
            print("Reading existing cache from %s" % cache_path)
            with open(cache_path) as infile:
                cache = json.loads(infile.read())

        num_empty = 0
        num_queries = 0

        with open(fixed_data_path) as infile:
            data = json.load(infile)

        for query in tqdm(data):
            for example in query["sentences"]:
                if example["question-split"] not in splits:
                    continue

                anon_sql = query["sql"][0]
                nl = example["text"]

                for variable, value in sorted(
                    example["variables"].items(),
                    key=lambda x: len(x[0]),
                    reverse=True,
                ):
                    if not value:
                        value = "%"
                    nl = nl.replace(variable, value)
                    anon_sql = anon_sql.replace(variable, value)
                anon_sql = anon_sql.replace('= "%"', 'LIKE "%"')
                anon_sql = anon_sql.replace("= %", 'LIKE "%"')

                if "scholar" in dataset_name.lower():
                    new_pred = ""
                    last_quote = ""
                    for char in anon_sql:
                        new_pred += char
                        if char in {'"', "'"} and not last_quote:
                            last_quote = char
                        elif char == last_quote:
                            last_quote = ""
                            new_pred += " COLLATE NOCASE"
                        anon_sql = new_pred

                if "advising" in dataset_name.lower():
                    # Fix so that it's selecting a concat of columns instead.
                    if "count" in anon_sql.lower():
                        # Find range of count thing
                        count_start_idx = anon_sql.lower().index("count")
                        count_end_idx = count_start_idx + anon_sql.lower()[
                            count_start_idx:
                        ].index(")")

                        if "," in anon_sql[count_start_idx:count_end_idx]:
                            problem_segment = anon_sql[
                                count_start_idx:count_end_idx
                            ]
                            problem_segment = problem_segment.replace(
                                ",", "||"
                            )
                            anon_sql = (
                                anon_sql[:count_start_idx]
                                + problem_segment
                                + anon_sql[count_end_idx:]
                            )
                    prev_token = ""
                    bad_tokens = set()
                    for token in anon_sql.split():
                        if prev_token == "=":
                            if (
                                token[0] in {'"', "'"}
                                and token[-1] in {'"', "'"}
                                and token[-2].isnumeric()
                                and not token[1].isnumeric()
                            ):
                                bad_tokens.add(token)
                            elif (
                                token[-1].isnumeric()
                                and not token[0].isnumeric()
                            ):
                                bad_tokens.add(token)
                            prev_token = token
                    for token in bad_tokens:
                        anon_sql = anon_sql.replace("= " + token, 'LIKE "%"')

                # Two specific exceptions on utterances that need correction or take a
                # long time to process.
                if nl == (
                    "What is the number of businesses user Michelle reviews per "
                    "month ?"
                ):
                    anon_sql = (
                        "select count(distinct(review.text)), review.month from "
                        "review where review.user_id in (select user_id from "
                        "user where user.name = 'Michelle') group by "
                        "review.month;"
                    )

                if nl == (
                    'return me the number of papers in " University of '
                    'Michigan " in Databases area .'
                ):
                    results = "121572"
                    cache[anon_sql] = results
                else:
                    if anon_sql not in cache:
                        # Update the cache to include this SQL query.
                        try:
                            c.execute(anon_sql)
                            results = c.fetchall()
                        except sqlite3.OperationalError as e:
                            with open(errors_filepath, "w+") as f:
                                f.write(nl + "\n")
                                f.write(anon_sql + "\n")
                                f.write(str(e) + "\n\n")

                            results = list()
                        cache[anon_sql] = results
                    else:
                        results = cache[anon_sql]

                    if not results:
                        num_empty += 1

                    if (
                        "advising" not in dataset_name
                        and nl in cache
                        and cache[nl] != anon_sql
                    ):
                        keep_going = (
                            input(
                                "Allow this to happen? This utterance will be "
                                "mapped to the second query."
                            ).lower()
                            == "y"
                        )
                        if not keep_going:
                            raise ValueError(
                                "NL is the same but anonymized SQL is not."
                            )
                cache[nl] = anon_sql
                num_queries += 1

    db.close()

    print("Writing cache")
    # with open(cache_path + '.tmp', 'w') as ofile:
    with open(cache_path, "w") as ofile:
        json.dump(cache, ofile)
    print(splits)


########################################################################################################################
# Code adapted from https://github.com/google-research/language/blob/master/language/xsp/data_preprocessing/michigan_preprocessing.py
def read_schema(schema_csv):
    """Loads a database schema from a CSV representation."""
    tables = {}
    with gfile.Open(schema_csv) as infile:
        for column in csv.DictReader(
            infile,
            quotechar='"',
            delimiter=",",
            quoting=csv.QUOTE_ALL,
            skipinitialspace=True,
        ):
            column = {
                key.lower().strip(): value
                for key, value in column.items()
                if key
            }

            table_name = column["table name"]
            if table_name != "-":
                if table_name not in tables:
                    tables[table_name] = list()
                column.pop("table name")
                tables[table_name].append(column)
    return tables


class SQLTokenizer(object):
    """A SQL tokenizer."""

    def __init__(self):
        pass

    def tokenize(self, sql):
        """Tokenizes a SQL query into a list of SQL tokens."""
        return [
            str(token).strip()
            for token in sqlparse.sql.TokenList(
                sqlparse.parse(sql)[0].tokens
            ).flatten()
            if str(token).strip()
        ]


def anonymize_aliases(sql):
    """Renames aliases to a consistent format (e.g., using T#)."""
    sql_tokens = list()
    tokens = SQLTokenizer().tokenize(sql)

    # First, split all TABLE.COLUMN examples into three tokens.
    for token in tokens:
        token = token.replace('"', "'")
        if (
            token != "."
            and token.count(".") == 1
            and not token.replace(".", "", 1).isnumeric()
        ):
            table, column = token.split(".")
            sql_tokens.extend([table, ".", column])
        else:
            sql_tokens.append(token)

    # Create an alias dictionary that maps from table names to column names
    alias_dict = dict()
    for token in sql_tokens:
        if "alias" in token and token not in alias_dict:
            alias_dict[token] = "T" + str(len(alias_dict) + 1)

    # Reconstruct the SQL query, this time replacing old alias names with the new
    # assigned alias names.
    new_tokens = list()
    for token in sql_tokens:
        if token in alias_dict:
            new_tokens.append(alias_dict[token])
        else:
            new_tokens.append(token)

    return new_tokens


def preprocess_sql(sql):
    """Preprocesses a SQL query into a clean string form."""
    return " ".join(anonymize_aliases(sql)).replace(" . ", ".")


def gen_predictions(
    run, fixed_data_path, splits, input_dir, mid_dir, output_filename
):
    with open(fixed_data_path) as q:
        queries_obj = json.load(q)

    out_lines = []
    count = 0

    schema_obj = read_schema(f"{input_dir}/{run}_schema.csv")
    for split in splits:
        print(f"Saving gold prediction data for {split} split.")

        # The UMichigan data is split by anonymized queries, where values are
        # anonymized but table/column names are not. However, our experiments are
        # performed on the original splits of the data.
        # count = 0
        for q_set in tqdm(queries_obj):
            # Take the first SQL query only. From their Github documentation:
            # "Note - we only use the first query, but retain the variants for
            #  completeness"
            # See https://github.com/google-research/language/blob/master/language/xsp/data_preprocessing/michigan_preprocessing.py#L44
            sql_txt = q_set["sql"][0]
            for example in q_set["sentences"]:
                if example["question-split"] not in split:
                    continue
                count += 1
                question = example["text"]
                query = sql_txt

                # Go through the anonymized values and replace them in both the natural
                # language and the SQL.
                #
                # It's very important to sort these in descending order. If one is a
                # substring of the other, it shouldn't be replaced first lest it ruin the
                # replacement of the superstring.
                for v_name, v_value in sorted(
                    example["variables"].items(),
                    key=lambda x: len(x[0]),
                    reverse=True,
                ):
                    if not v_value:
                        # From https://github.com/google-research/language/blob/master/language/xsp/data_preprocessing/michigan_preprocessing.py#L67:
                        # TODO(alanesuhr) While the Michigan repo says to use a - here, the
                        # thing that works is using a % and replacing = with LIKE.
                        #
                        # It's possible that I should remove such clauses from the SQL, as
                        # long as they lead to the same table result. They don't align well
                        # to the natural language at least.
                        #
                        # See: https://github.com/jkkummerfeld/text2sql-data/tree/master/data
                        v_value = "%"
                    question = question.replace(v_name, v_value)
                    query = query.replace(v_name, v_value)

                # In the case that we replaced an empty anonymized value with %, make it
                # compilable new allowing equality with any string.
                query = query.replace('= "%"', 'LIKE "%"')
                query = query.replace("= %", 'LIKE "%"')

                sent_obj = {
                    "utterance": question,
                    "predictions": [],
                    "scores": [],
                    "gold": query,
                    "database_path": f"databases/{run}.db",
                    "empty_database_path": f"empty_databases/{run}.db",
                    "schema": schema_obj,
                }
                out_lines.append(sent_obj)

    print(f"  Writing out {output_filename} file for {run}: {count} examples")
    with open(f"{mid_dir}/{output_filename}", "w+") as o:
        json.dump(out_lines, o)


########################################################################################################################
# Code adapted from https://github.com/google-research/language/blob/master/language/xsp/evaluation/official_evaluation.py
"""Official evaluation script for natural language to SQL datasets.

Arguments:
  predictions_filepath (str): Path to a predictions file (in JSON format).
  output_filepath (str): Path to the file where the result of execution is
    saved.
  cache_filepath (str): Path to a JSON file containing a mapping from gold SQL
    queries to cached resulting tables.  Should be ran locally. All filepaths
    above should refer to the local filesystem.
"""
no_cache = []

# Maximum allowable timeout for executing predicted and gold queries.
TIMEOUT = 60

# Maximum number of candidates we should consider
MAX_CANDIDATE = 20

# These are substrings of exceptions from sqlite3 that indicate certain classes
# of schema and syntax errors.
SCHEMA_INCOHERENCE_STRINGS = {
    "no such table",
    "no such column",
    "ambiguous column name",
}
SYNTAX_INCORRECTNESS_STRINGS = {
    "bad syntax",
    "unrecognized token",
    "incomplete input",
    "misuse of aggregate",
    "left and right",
    "wrong number of arguments",
    "sub-select returns",
    "1st order by term does not match any column",
    "no such function",
    "clause is required before",
    "incorrect number of bindings",
    "datatype mismatch",
    "syntax error",
}


def normalize_sql_str(string):
    """Normalizes the format of a SQL string for string comparison."""
    string = string.lower()
    while "  " in string:
        string = string.replace("  ", " ")
    string = string.strip()
    string = string.replace("( ", "(").replace(" )", ")")
    string = string.replace(" ;", ";")
    string = string.replace('"', "'")

    if ";" not in string:
        string += ";"
    return string


def string_acc(s1, s2):
    """Computes string accuracy between two SQL queries."""
    return normalize_sql_str(s1) == normalize_sql_str(s2)


def result_table_to_string(table):
    """Converts a resulting SQL table to a human-readable string."""
    string_val = (
        "\t"
        + "\n\t".join([str(row) for row in table[: min(len(table), 5)]])
        + "\n"
    )
    if len(table) > 5:
        string_val += "... and %d more rows.\n" % (len(table) - 5)
    return string_val


def try_executing_query(
    prediction, cursor, case_sensitive=True, verbose=False
):
    """Attempts to execute a SQL query against a database given a cursor."""
    exception_str = None

    prediction_str = prediction[:]
    prediction_str = prediction_str.replace(";", "").strip()

    st = time.time()
    try:
        if not case_sensitive:
            new_prediction = ""
            last_quote = ""
            for char in prediction:
                new_prediction += char
                if char in {'"', "'"} and not last_quote:
                    last_quote = char
                elif char == last_quote:
                    last_quote = ""
                    new_prediction += " COLLATE NOCASE"
            prediction = new_prediction

            if verbose:
                print("Executing case-insensitive query:")
                print(new_prediction)
        pred_results = timeout_execute(cursor, prediction)
    except timeout_decorator.timeout_decorator.TimeoutError:
        print("!time out!")
        pred_results = []
        exception_str = "timeout"
    except (
        sqlite3.Warning,
        sqlite3.Error,
        sqlite3.DatabaseError,
        sqlite3.IntegrityError,
        sqlite3.ProgrammingError,
        sqlite3.OperationalError,
        sqlite3.NotSupportedError,
    ) as e:
        exception_str = str(e).lower()
        pred_results = []
    execution_time = time.time() - st

    return pred_results, exception_str, execution_time


@timeout_decorator.timeout(seconds=TIMEOUT, use_signals=False)
def timeout_execute(cursor, prediction):
    cursor.execute(prediction)
    pred_results = cursor.fetchall()
    pred_results = [list(result) for result in pred_results]
    return pred_results


def find_used_entities_in_string(query, columns, tables):
    """Heuristically finds schema entities included in a SQL query."""
    used_columns = set()
    used_tables = set()

    nopunct_query = query.replace(".", " ").replace("(", " ").replace(")", " ")

    for token in nopunct_query.split(" "):
        if token.lower() in columns:
            used_columns.add(token.lower())
        if token.lower() in tables:
            used_tables.add(token.lower())
    return used_columns, used_tables


def compute_f1(precision, recall):
    if precision + recall > 0.0:
        return 2 * precision * recall / (precision + recall)
    else:
        return 0.0


def compute_set_f1(pred_set, gold_set):
    """Computes F1 of items given two sets of items."""
    prec = 1.0
    if pred_set:
        prec = float(len(pred_set & gold_set)) / len(pred_set)

    rec = 1.0
    if gold_set:
        rec = float(len(pred_set & gold_set)) / len(gold_set)
    return compute_f1(prec, rec)


def col_tab_f1(schema, gold_query, predicted_query):
    """Computes the F1 of tables and columns mentioned in the two queries."""

    # Get the schema entities.
    db_columns = set()
    db_tables = set()
    for name, cols in schema.items():
        for col in cols:
            db_columns.add(col["field name"].lower())
        db_tables.add(name.lower())

    # Heuristically find the entities used in the gold and predicted queries.
    pred_columns, pred_tables = find_used_entities_in_string(
        predicted_query, db_columns, db_tables
    )
    gold_columns, gold_tables = find_used_entities_in_string(
        gold_query, db_columns, db_tables
    )

    # Compute and return column and table F1.
    return (
        compute_set_f1(pred_columns, gold_columns),
        compute_set_f1(pred_tables, gold_tables),
    )


def execute_prediction(
    prediction, empty_table_cursor, cursor, case_sensitive, verbose
):
    """Executes a single example's prediction(s).

    If more than one prediction is available, the most likely executable
    prediction is used as the "official" prediction.

    Args:
      prediction: A dictionary containing information for a single example's
        prediction.
      empty_table_cursor: The cursor to a database containing no records, to be
        used only to determine whether a query is executable in the database.
      cursor: The sqlite3 database cursor to execute queries on.
      case_sensitive: Boolean indicating whether the execution should be case
        sensitive with respect to string values.
      verbose: Whether to print details about what queries are being executed.

    Returns:
      Tuple containing the highest-ranked executable query, the resulting table,
      and any exception string associated with executing this query.
    """

    # Go through predictions in order of probability and test their executability
    # until you get an executable prediction. If you don't find one, just
    # "predict" the most probable one.
    paired_preds_and_scores = zip(
        prediction["predictions"], prediction["scores"]
    )
    sorted_by_scores = sorted(
        paired_preds_and_scores, key=lambda x: x[1], reverse=True
    )

    best_prediction = None
    pred_results = None
    exception_str = None
    execution_time = 0

    if len(sorted_by_scores) > MAX_CANDIDATE:
        sorted_by_scores = sorted_by_scores[:MAX_CANDIDATE]

    for i, (pred, _) in enumerate(sorted_by_scores):
        # Try predicting
        if verbose:
            print("Trying to execute query:\n\t" + pred)
            print("... on empty database")
        temp_exception_str = try_executing_query(
            pred, empty_table_cursor, case_sensitive, verbose
        )[1]

        if temp_exception_str:
            if i == 0:
                # By default, set the prediction to the first (highest-scoring)
                # one.
                best_prediction = pred

                # Get the actual results
                if verbose:
                    print("... on actual database")
                (
                    pred_results,
                    exception_str,
                    execution_time,
                ) = try_executing_query(pred, cursor, case_sensitive, verbose)
            if exception_str == "timeout":
                # Technically, this query didn't have a syntax problem, so
                # continue and set this as the best prediction.
                best_prediction = pred

                if verbose:
                    print("... on actual database")
                (
                    pred_results,
                    exception_str,
                    execution_time,
                ) = try_executing_query(pred, cursor, case_sensitive, verbose)
                break
        else:
            best_prediction = pred
            exception_str = None

            if verbose:
                print("No exception... on actual database")
            pred_results, _, execution_time = try_executing_query(
                pred, cursor, case_sensitive, verbose
            )
            break

    return best_prediction, pred_results, exception_str, execution_time


def execute_predictions(
    predictions, cache_dict, ofile, case_sensitive, verbose, update_cache
):
    """Executes predicted/gold queries and computes performance.

    Writes results to ofile.

    Args:
      predictions: A list of dictionaries defining the predictions made by a
        model.
      cache_dict: A dictionary mapping from gold queries to the resulting tables.
      ofile: A file pointer to be written to.
      case_sensitive: A Boolean indicating whether execution of queries should be
        case sensitive with respect to strings.
      verbose: Whether to print detailed information about evaluation (e.g., for
        debugging).
      update_cache: Whether to execute and cache gold queries.
    """
    # Keeps tracks of metrics throughout all of the evaluation.
    exec_results_same = list()
    string_same = list()

    precision = list()
    recall = list()

    column_f1s = list()
    table_f1s = list()

    conversion_errors = 0

    schema_errors = 0
    syntax_errors = 0
    timeouts = 0

    gold_error = 0

    i = 0

    predictions_iterator = tqdm
    if verbose:
        # Don't use TQDM if verbose: it might mess up the verbose messages
        predictions_iterator = lambda x: x

    for prediction in predictions_iterator(predictions):
        # Attempt to connect to the database for executing.
        try:
            conn = sqlite3.connect(prediction["database_path"])
            conn.text_factory = str
        except sqlite3.OperationalError as e:
            print(prediction["database_path"])
            raise e

        empty_path = prediction["empty_database_path"]
        try:
            empty_conn = sqlite3.connect(empty_path)
            empty_conn.text_factory = str
        except sqlite3.OperationalError as e:
            print(empty_path)
            raise e

        empty_cursor = empty_conn.cursor()
        cursor = conn.cursor()

        ofile.write("Example #" + str(i) + "\n")
        printable_utterance = prediction["utterance"]
        ofile.write(printable_utterance + "\n")

        if verbose:
            print(
                "Finding the highest-rated prediction for utterance:\n\t"
                + printable_utterance
            )

        (
            best_prediction,
            pred_results,
            exception_str,
            execution_time,
        ) = execute_prediction(
            prediction, empty_cursor, cursor, case_sensitive, verbose
        )

        ofile.write("Predicted query:\n")
        if best_prediction:
            ofile.write("\t" + best_prediction.strip() + "\n")
        else:
            ofile.write(
                "ERROR: Cannot write prediction %r\n" % best_prediction
            )

        # If it didn't execute correctly, check why.
        if exception_str:
            ofile.write(exception_str + "\n")

            found_error = False
            for substring in SCHEMA_INCOHERENCE_STRINGS:
                if substring in exception_str.lower():
                    schema_errors += 1
                    found_error = True
                    break

            if not found_error:
                for substring in SYNTAX_INCORRECTNESS_STRINGS:
                    if substring in exception_str.lower():
                        syntax_errors += 1
                        found_error = True
                        break

            if not found_error and "timeout" in exception_str:
                ofile.write("Execution (predicted) took too long.\n")
                found_error = True
                timeouts += 1

            # If the error type hasn't been identified, exit and report it.
            if not found_error:
                print(best_prediction)
                print(exception_str)
                exit(1)

            # Predicted table should be empty for all of these cases.
            pred_results = []

        # Compare to gold and update metrics
        gold_query = prediction["gold"]

        ofile.write("Gold query:\n")
        ofile.write("\t" + gold_query.strip() + "\n")

        # Get the gold results
        if not case_sensitive:
            new_pred = ""
            last_quote = ""
            for char in gold_query:
                new_pred += char
                if char in {'"', "'"} and not last_quote:
                    last_quote = char
                elif char == last_quote:
                    last_quote = ""
                    new_pred += " COLLATE NOCASE"
                gold_query = new_pred
        if cache_dict is None or gold_query not in cache_dict:
            if printable_utterance not in cache_dict:
                if update_cache:
                    if verbose:
                        print(
                            "Trying to execute the gold query:\n\t"
                            + gold_query
                        )
                    (
                        gold_results,
                        gold_exception_str,
                        execution_time,
                    ) = try_executing_query(
                        gold_query, cursor, case_sensitive, verbose
                    )

                    if gold_exception_str:
                        gold_error += 1
                        gold_results = []
                    elif cache_dict is not None:
                        cache_dict[gold_query] = gold_results
                else:
                    print(gold_query)
                    print(printable_utterance)
                    raise ValueError("Cache miss!")

        gold_results = cache_dict.get(gold_query, None)
        if gold_results == None:
            no_cache.append(gold_query)

        if best_prediction:
            string_same.append(string_acc(gold_query, best_prediction))
            col_f1, tab_f1 = col_tab_f1(
                prediction["schema"], gold_query, best_prediction
            )
            column_f1s.append(col_f1)
            table_f1s.append(tab_f1)
            ofile.write("Column F1: %f\n" % col_f1)
            ofile.write("Table F1: %f\n" % tab_f1)

            if "order by" in gold_query:
                results_equivalent = pred_results == gold_results
            else:
                pred_set = set()
                gold_set = set()
                for pred in pred_results:
                    if isinstance(pred, list):
                        pred_set.add(" ".join([str(item) for item in pred]))
                    else:
                        pred_set.add(pred)
                for gold in gold_results:
                    if isinstance(gold, list):
                        gold_set.add(" ".join([str(item) for item in gold]))
                    else:
                        gold_set.add(gold)

                results_equivalent = pred_set == gold_set

        else:
            string_same.append(0.0)
            ofile.write("Column F1: 0.")
            ofile.write("Table F1: 0.")
            column_f1s.append(0.0)
            table_f1s.append(0.0)

            conversion_errors += 1

            # Only consider correct if the gold table was empty.
            results_equivalent = gold_results == list()

        exec_results_same.append(int(results_equivalent))
        ofile.write("Execution was correct? " + str(results_equivalent) + "\n")

        # Add some debugging information about the tables, and compute the
        # precisions.
        if pred_results:
            if not results_equivalent:
                ofile.write("Predicted table:\n")
                ofile.write(result_table_to_string(pred_results))

            precision.append(int(results_equivalent))
        elif best_prediction is None or not results_equivalent:
            ofile.write("Predicted table was EMPTY!\n")

        if gold_results:
            ofile.write("Gold table:\n")
            ofile.write(result_table_to_string(gold_results))

            recall.append(int(results_equivalent))
        else:
            ofile.write("Gold table was EMPTY!\n")

        ofile.write("\n")
        ofile.flush()

        conn.close()
        empty_conn.close()

        i += 1

    # Write the overall metrics to the file.
    num_empty_pred = len(precision)
    num_empty_gold = len(recall)

    precision = np.mean(np.array(precision))
    recall = np.mean(np.array(recall))

    execution_f1 = compute_f1(precision, recall)

    ofile.write(
        "String accuracy: "
        + "{0:.2f}".format(100.0 * np.mean(np.array(string_same)))
        + "\n"
    )
    ofile.write(
        "Accuracy: "
        + "{0:.2f}".format(100.0 * np.mean(np.array(exec_results_same)))
        + "\n"
    )
    ofile.write(
        "Precision: "
        + "{0:.2f}".format(100.0 * precision)
        + " ; "
        + str(num_empty_pred)
        + " nonempty predicted tables"
        + "\n"
    )
    ofile.write(
        "Recall: "
        + "{0:.2f}".format(100.0 * recall)
        + " ; "
        + str(num_empty_gold)
        + " nonempty gold tables"
        + "\n"
    )
    ofile.write(
        "Execution F1: " + "{0:.2f}".format(100.0 * execution_f1) + "\n"
    )
    ofile.write(
        "Timeout: "
        + "{0:.2f}".format(timeouts * 100.0 / len(predictions))
        + "\n"
    )
    ofile.write(
        "Gold did not execute: "
        + "{0:.2f}".format(gold_error * 100.0 / len(predictions))
        + "\n"
    )
    ofile.write(
        "Average column F1: "
        + "{0:.2f}".format(100.0 * np.mean(np.array(column_f1s)))
        + "\n"
    )
    ofile.write(
        "Average table F1: "
        + "{0:.2f}".format(100.0 * np.mean(np.array(table_f1s)))
        + "\n"
    )
    ofile.write(
        "Schema errors: "
        + "{0:.2f}".format((schema_errors) * 100.0 / len(predictions))
        + "\n"
    )
    ofile.write(
        "Syntax errors:  "
        + "{0:.2f}".format((syntax_errors) * 100.0 / len(predictions))
        + "\n"
    )
    ofile.write(
        "Conversion errors: "
        + "{0:.2f}".format((conversion_errors * 100.0) / len(predictions))
        + "\n"
    )


def run_evaluation(
    predictions_filepath,
    output_filepath,
    cache_filepath,
    verbose,
    update_cache,
):
    # Load the predictions filepath.
    with open(predictions_filepath) as infile:
        predictions = json.load(infile)
    # print('Loaded %d predictions.' % len(predictions))

    # Load or create the cache dictionary mapping from gold queries to resulting
    # tables.
    cache_dict = None

    # Only instantiate the cache dict if using Spider.
    # print('cache path: ' + cache_filepath)

    basefilename = os.path.basename(predictions_filepath).lower()

    if "spider" not in basefilename:
        cache_dict = dict()
        if os.path.exists(cache_filepath):
            # print('Loading cache from %s' % cache_filepath)
            with open(cache_filepath) as infile:
                cache_dict = json.load(infile)
            # print('Loaded %d cached queries' % len(cache_dict))

    # Create the text file that results will be written to.
    with open(output_filepath, "w") as ofile:
        execute_predictions(
            predictions,
            cache_dict,
            ofile,
            "scholar" not in basefilename,
            verbose,
            update_cache,
        )

    if "spider" not in basefilename:
        try:
            cache_str = json.dumps(cache_dict)
            with open(cache_filepath, "w") as ofile:
                ofile.write(cache_str)
        except UnicodeDecodeError as e:
            print("Could not save the cache dict. Exception:")
            print(e)

    print("==================================")
    print(f"No cache entries for {len(no_cache)} queries:")
    print(no_cache)


########################################################################################################################
# Code adapted from https://github.com/google-research/language/blob/master/language/xsp/evaluation/filter_results.py
"""Filters the results of running evaluation to include only clean examples.

It filters the following:

- Examples where the resulting table of the gold query is the empty table
- Examples where the resulting table of the gold query is [0], if the gold
  query returns a count
- Examples where strings or numerical values in the gold query are not present
  in the input utterance
- Examples where multiple columns are selected in the resulting table

Usage:
    The argument is the *_eval.txt file that is generated by
    official_evaluation.py.

"""


def get_nlqs_to_remove(eval_filename):
    with open(eval_filename) as infile:
        examples = infile.read().split("\n\n")

    num_exec_correct = 0
    num_filtered = 0
    num_removed = 0
    filtered_utterances = {}
    for example in examples[:-1]:
        nlq = example.split("\n")[1]

        # Filter out examples with empty gold tables.
        if "Gold table was EMPTY!" in example:
            filtered_utterances[nlq] = 1
            num_removed += 1
            continue

        # Filter out examples with a result of [0] and that require a count.
        if example.endswith("Gold table:\n\t[0]") and (
            "gold query:\n\tselect count" in example.lower()
            or "gold query:\n\tselect distinct count" in example.lower()
        ):
            filtered_utterances[nlq] = 1
            num_removed += 1
            continue

        # Filter out examples that require copying values that can't be copied.
        prev_value = ""
        example_lines = example.split("\n")
        last_quote = ""
        gold_query_idx = example_lines.index("Gold query:") + 1
        utterance = example_lines[1]
        copiable = True
        in_equality = False
        numerical_value = ""
        handled_prefix = False
        too_many_selects = False
        gold_query = example_lines[gold_query_idx].strip()

        for i, char in enumerate(gold_query):
            # Check that it's only selecting a single table at the top
            if (
                not handled_prefix
                and i - 4 >= 0
                and gold_query[i - 4 : i].lower() == "from"
            ):
                handled_prefix = True
                if gold_query[:i].count(",") > 0:
                    too_many_selects = True

            if char == last_quote:
                last_quote = ""

                prev_value = prev_value.replace("%", "")

                if prev_value not in utterance:
                    copiable = False

                prev_value = ""

            elif last_quote:
                prev_value += char
            elif char in {'"', "'"}:
                last_quote = char

            if char in {"=", ">", "<"}:
                in_equality = True

            if in_equality:
                if char.isdigit() or char == ".":
                    if numerical_value or (
                        not prev_value and gold_query[i - 1] == " "
                    ):
                        numerical_value += char

                if char == " " and numerical_value:
                    in_equality = False

                    if (
                        numerical_value not in utterance
                        and numerical_value not in {"0", "1"}
                    ):
                        # Allow generation of 0, 1 for compositionality purposes.
                        copiable = False
                    numerical_value = ""

        if not copiable or too_many_selects:
            filtered_utterances[nlq] = 1
            num_removed += 1
            continue

        num_filtered += 1

        if "Execution was correct? True" in example:
            num_exec_correct += 1

    print(
        "Filtered from %d to %d examples" % (len(examples) - 1, num_filtered)
    )
    print(f"Removed {num_removed} examples")
    return filtered_utterances


if __name__ == "__main__":
    main()
