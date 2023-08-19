#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Author: Mingwen Dong

import argparse
import json
import os, shutil
import sqlite3
from collections import Counter
import string
import re
import argparse


SQL_KEYWORDS_SET = set(
    [
        "order",
        "view",
        "select",
        "from",
        "group",
        "exists",
        "index",
        "drop",
        "top",
        "set",
        "values",
        "union",
        "unique",
        "top",
        "or",
        "limit",
        "like",
        "where",
        "not",
        "join",
        "from",
        "desc",
        "default",
        "database",
        "delete",
        "distinct",
        "check",
        "case",
        "alter",
        "any",
        "all",
        "add",
        "asc",
        "as",
        "in",
        "table",
        "returning",
        "return",
    ]
)


def is_float(text):
    try:
        float(text)
        return True
    except:
        return False


def normalize_string(in_str, prefix=None):
    in_str = in_str.strip()
    regex = re.compile("[%s]" % re.escape(string.punctuation))
    in_str = regex.sub(" ", in_str)
    orginal_str = "_".join(in_str.split()).lower()
    normalize_str = orginal_str.replace("_", " ")
    normalize_str = " ".join(simple_tokenizer(normalize_str))
    if prefix:
        orginal_str = prefix + "_" + orginal_str
    if orginal_str == "":
        orginal_str = "none"

    if normalize_str == "":
        normalize_str = "none"

    return orginal_str, normalize_str


def correct_invalid_names(name: str):
    """
    A valid SQL name for columns, tables, and databases must follow the rules below:
        - Names must begin with an underscore (_) or an alphabetic character and must contain only alphanumeric characters
        - A name can contain but not begin with 0 – 9, @, #, and $. Nevertheless, names in double-quotes/delimited identifiers can have additional special characters.

    if a table name starts with number/year (e.g., 2006–07_toronto_raptors_season_game_log), add double quotes.
    if a column name starts with number (e.g., 1953), add double quotes.
    if a column/table name is SQL keyword, add double quotes
    """
    if re.search(r"^[\d]|@|#|\$", name) or name.lower() in SQL_KEYWORDS_SET:
        name = '"' + name + '"'
    return name


def correct_invalid_names_v2(name: str, special_prefix):
    """
    A valid SQL name for columns, tables, and databases must follow the rules below:
        - Names must begin with an underscore (_) or an alphabetic character and must contain only alphanumeric characters
        - A name can contain but not begin with 0 – 9, @, #, and $. Nevertheless, names in double-quotes/delimited identifiers can have additional special characters.

    if a table name starts with number/year (e.g., 2006–07_toronto_raptors_season_game_log), add double quotes.
    if a column name starts with number (e.g., 1953), add double quotes.
    if a column/table name is SQL keyword, add double quotes
    """
    nname = (
        name.replace("\n", "_")
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
    )

    if re.search(r"^\d{4}\_", nname):
        nname = "year_" + nname

    elif (
        name.lower() in SQL_KEYWORDS_SET
        or re.search(r"^\b(to|table|returning|return|in|where|from)\b", name)
        or re.search(r"^[\d]|@|#|\$", name)
    ):
        # elif re.search(r"^[\d]|@|#|\$|to|table|returning|return|in|where|from", name) or name.lower() in SQL_KEYWORDS_SET:
        nname = special_prefix + "_" + nname

    return nname


def remove_parathesis_from_sql(name: str) -> str:
    """
    paratheses were added to allow invalid column table names. but it may not be needed for SQL?
    """
    if name.startswith('"') and name.endswith('"'):
        return name[1:-1]
    return name


def simple_tokenizer(text):
    return re.findall(r"\w+|[^\w\s]", text, re.UNICODE)


def infer_column_type_from_contents(rows):
    column_types = ["number"] * len(rows[0])
    for row in rows:
        for i, cell in enumerate(row):
            column_types[i] = "text" if not is_float(cell) else "number"
    return column_types


def convert_wikisql_schema_into_spider_schema(
    table_json, database_dir, header2skip=[], default_table_name=None
):
    """
    {
       "header" : [
          "Player",
          "No.",
          "Nationality",
          "Position",
          "Years in Toronto",
          "School/Club Team"
       ],
       "id" : "1-10015132-11",
       "rows" : [
          [
             "Antonio Lang",
             "21",
             "United States",
             "Guard-Forward",
             "1999-2000",
             "Duke"
          ],
          [
             "Voshon Lenard",
             "2",
             "United States",
             "Guard",
             "2002-03",
             "Minnesota"
          ],

        ],
       "types" : [
          "text",
          "text",
          "text",
          "text",
          "text",
          "text"
       ],
       "section_title" : "L",
       "page_title" : "Toronto Raptors all-time roster",
       "name" : "table_10015132_11",
       "caption" : "L"
    }


    target format
    {
        "column_names": [
          [
            0,
            "id"
          ],
          [
            0,
            "name"
          ],
          [
            0,
            "country code"
          ],
          [
            0,
            "district"
          ],
          .
          .
          .
        ],
        "column_names_original": [
          [
            0,
            "ID"
          ],
          [
            0,
            "Name"
          ],
          [
            0,
            "CountryCode"
          ],
          [
            0,
            "District"
          ],
          .
          .
          .
        ],
        "column_types": [
          "number",
          "text",
          "text",
          "text",
             .
             .
             .
        ],
        "db_id": "world_1",
        "foreign_keys": [
          [
            3,
            8
          ],
          [
            23,
            8
          ]
        ],
        "primary_keys": [
          1,
          8,
          23
        ],
        "table_names": [
          "city",
          "sqlite sequence",
          "country",
          "country language"
        ],
        "table_names_original": [
          "city",
          "sqlite_sequence",
          "country",
          "countrylanguage"
        ]
      }
    """
    # header = table_json['header']
    # id = table_json['id']
    types = table_json["types"]
    inferred_types = infer_column_type_from_contents(table_json["rows"])
    if inferred_types:
        for i in range(len(inferred_types)):
            types[i] = inferred_types[i]
    table_json["originalHeader"] = table_json["header"]
    table_json["header"] = [col for col in table_json["header"] if col not in header2skip]

    if len(table_json["header"]) == 0:
        return None

    if default_table_name is None:
        page_title = table_json["page_title"] if "page_title" in table_json.keys() else "page_title"
        section_title = (
            table_json["section_title"] if "section_title" in table_json.keys() else "section_title"
        )
        # table name
        table_name = page_title + " " + section_title
    else:
        table_name = default_table_name

    table_names = []
    table_names_original = []
    column_names = []
    column_names_original = []
    column_types = []

    # original_table_name, normalized_table_name = normalize_string(
    #     table_name, prefix='t')
    original_table_name, normalized_table_name = normalize_string(table_name, prefix=None)
    original_table_name = correct_invalid_names_v2(original_table_name, "table")

    table_names.append(normalized_table_name)
    table_names_original.append(original_table_name)

    # column names
    column_name_counter = Counter()
    for i, column_name in enumerate(table_json["header"]):
        # original_column_name, normalized_column_name = normalize_string(
        #     column_name, prefix='c')
        original_column_name, normalized_column_name = normalize_string(column_name, prefix=None)
        original_column_name = correct_invalid_names_v2(original_column_name, "col")

        cur_count = column_name_counter[original_column_name]
        column_name_counter.update([original_column_name])
        if cur_count > 0:
            original_column_name = original_column_name + "_%d" % cur_count
        column_names.append([0, normalized_column_name])
        column_names_original.append([0, original_column_name])
        column_types.append(types[i])

    schema = {
        "db_id": table_json["id"],
        "table_names": table_names,
        "table_names_original": table_names_original,
        "column_names": column_names,
        "column_names_original": column_names_original,
        "column_types": column_types,
        "foreign_keys": [],
        "primary_keys": [],
    }

    # create directory to store database
    dir_path = os.path.join(database_dir, table_json["id"])
    try:
        os.makedirs(dir_path)
    except OSError:
        print("Creation of the directory %s failed" % dir_path)
    else:
        # print("Successfully created the directory %s " % dir_path)
        pass

    # Create table
    cur_database_path = os.path.join(dir_path, table_json["id"] + ".sqlite")
    conn = sqlite3.connect(cur_database_path)
    c = conn.cursor()
    names = [column[1] for column in column_names_original]
    cmd = "CREATE TABLE " + original_table_name + " (" + ", ".join(names) + ")"
    # print(header)
    print(cmd)
    try:
        c.execute(cmd)
        # add items into table
        cmd = (
            "INSERT INTO "
            + original_table_name
            + " VALUES ("
            + ", ".join(["?"] * len(column_names_original))
            + ")"
        )
        rows = [tuple(row) for row in table_json["rows"]]
        if any(rows):
            c.executemany(cmd, rows)
    except sqlite3.OperationalError as ex:
        print(f"Table already exists. Skip this table. {ex}")

    # Save (commit) the changes
    conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()

    return schema


def convert_wikisql_questions_into_spider_format(question, schema):
    """
        input format
        {
       "table_id" : "1-10015132-11",
       "question" : "What position does the player who played for butler cc (ks) play?",
       "phase" : 1,
       "sql" : {
          "conds" : [
             [
                5,
                0,
                "Butler CC (KS)"
             ]
          ],
          "sel" : 3,
          "agg" : 0
       }
    }
    """
    agg_ops = ["", "MAX", "MIN", "COUNT", "SUM", "AVG"]
    cond_ops = ["=", ">", "<", "OP"]
    syms = [
        "SELECT",
        "WHERE",
        "AND",
        "COL",
        "TABLE",
        "CAPTION",
        "PAGE",
        "SECTION",
        "OP",
        "COND",
        "QUESTION",
        "AGG",
        "AGGOPS",
        "CONDOPS",
    ]

    statement = []

    # select clause
    statement.append("SELECT")
    select_column_id = question["sql"]["sel"]
    select_column_name = schema["column_names_original"][select_column_id][1]

    agg_type = agg_ops[question["sql"]["agg"]]
    if agg_type == "":
        statement.append(select_column_name)
    else:
        statement.append(agg_type + "(" + select_column_name + ")")

    # table name
    table_name = schema["table_names_original"][0]
    statement.append("FROM")
    statement.append(table_name)

    for i, cond in enumerate(question["sql"]["conds"]):
        if i == 0:
            statement.append("WHERE")
        else:
            statement.append("AND")

        cur_column_id = cond[0]
        cur_column_name = schema["column_names_original"][cur_column_id][1]
        operator = cond_ops[cond[1]]
        condition = cond[2]
        statement.append(cur_column_name)
        statement.append(operator)
        if is_float(condition):
            statement.append(str(condition))
        # else:
        #     statement.append(condition)
        else:
            if isinstance(condition, str):
                if condition.count('"') > 2 or condition.count('"') == 1:
                    condition = condition.replace('"', '"')

                else:
                    if condition.startswith('"') and condition.endswith('"'):
                        condition = condition.strip('"')
                    if condition.startswith("'") and condition.endswith("'"):
                        condition = condition.strip("'")
            else:
                condition = str(condition)
            # if len(condition.split()) == 1:
            #     statement.append(str(condition))
            # elif len(condition.split()) > 1:
            #     statement.append('"' + condition + '"')
            statement.append('"' + condition + '"')

    statement = " ".join(statement)
    new_question = {
        "db_id": question["table_id"],
        "question": question["question"],
        "query": statement,
    }
    return new_question


def convert_all_schemas(
    base_dir="",
    in_table_paths=[],
):
    database_dir = os.path.join(base_dir, "database")
    in_paths = in_table_paths
    if os.path.exists(database_dir):
        shutil.rmtree(database_dir, ignore_errors=True)
    all_schemas = []
    for in_path in in_paths:
        in_file = open(in_path, "rt", encoding="utf-8")
        for line in in_file:
            table_json = json.loads(line.strip())
            schema = convert_wikisql_schema_into_spider_schema(table_json, database_dir)
            all_schemas.append(schema)
        in_file.close()
    schema_path = os.path.join(base_dir, "wikisql_schema.json")
    with open(schema_path, "wt", encoding="utf-8") as fout:
        json.dump(all_schemas, fout, indent=2)


def convert_all_questions(
    base_dir="",
    in_question_paths=[],
):
    # load schema
    schema_path = os.path.join(base_dir, "wikisql_schema.json")
    with open(schema_path, "rt", encoding="utf-8") as in_file:
        schema_list = json.load(in_file)
    id_schema = dict()
    for schema in schema_list:
        id_schema[schema["db_id"]] = schema

    # convert questions
    in_paths = {os.path.basename(fp).replace(".jsonl", ""): fp for fp in in_question_paths}

    for dataset in in_paths.keys():
        in_path = in_paths[dataset]
        out_path = os.path.join(base_dir, f"{dataset}.jsonl")
        print(in_path)
        in_file = open(in_path, "rt", encoding="utf-8")
        out_file = open(out_path, "wt", encoding="utf-8")
        for line in in_file:
            question = json.loads(line.strip())
            schema = id_schema[question["table_id"]]
            new_question = convert_wikisql_questions_into_spider_format(question, schema)
            out_file.write(json.dumps(new_question) + "\n")
        in_file.close()
        out_file.close()


def merge_wiki_sql_and_spider_schemas(
    base_dir="",
):
    wiki_sql_path = os.path.join(base_dir, "wikisql_schema.json")
    spider_path = os.path.join(base_dir, "spider_tables.json")

    all_schemas = list()

    with open(wiki_sql_path, "rt", encoding="utf_8") as in_file:
        wiki_schemas = json.load(in_file)
    for wiki_schema in wiki_schemas:
        wiki_schema["column_names"].insert(0, [-1, "*"])
        wiki_schema["column_names_original"].insert(0, [-1, "*"])
        wiki_schema["column_types"].insert(0, "text")
        all_schemas.append(wiki_schema)

    table_path = os.path.join(base_dir, "tables.json")
    with open(table_path, "wt", encoding="utf_8") as out_file:
        json.dump(all_schemas, out_file)


def has_number(in_str):
    for char in str(in_str):
        if char in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
            return True
    return False


def collect_all_cell_values(in_path, out_path):
    in_file = open(in_path, "rt", encoding="utf-8")
    all_values = set()
    for line in in_file:
        question = json.loads(line.strip())
        for i, cond in enumerate(question["sql"]["conds"]):
            condition = cond[2]
            if has_number(condition):
                continue
            else:
                all_values.add(condition)
    in_file.close()
    out_file = open(out_path, "wt", encoding="utf-8")
    out_file.write("\n".join(list(all_values)))
    out_file.close()


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--base_out_dir",
        type=str,
        help="output folder where output data will be written to",
        default="unified/wikisql",
    )
    parser.add_argument(
        "--original_data_dir",
        type=str,
        help="directory to extracted data files from `https://github.com/salesforce/WikiSQL/blob/master/data.tar.bz2`",
        default="original/wikisql",
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    # download and extract wikisql data files from `https://github.com/salesforce/WikiSQL/blob/master/data.tar.bz2` following the readme on WikiSQL
    # base_out_dir the output folder where output data will be written to
    base_out_dir = args.base_out_dir

    # directory to extracted data files from `https://github.com/salesforce/WikiSQL/blob/master/data.tar.bz2`
    # it shall contain both the *.tables.jsonl and *.jsonl files
    original_wikisql_data_dir = args.original_data_dir
    splits_to_convert = ["test", "dev", "train"]

    # in_table_paths are where the original wikisql `.jsonl` tables are stored. E.g., `dev.tables.jsonl`, `test.tables.jsonl`, `train.tables.jsonl`
    in_table_paths = [
        os.path.join(original_wikisql_data_dir, f"{split}.tables.jsonl")
        for split in splits_to_convert
    ]
    convert_all_schemas(base_dir=base_out_dir, in_table_paths=in_table_paths)

    # in_question_paths are where the original wikisql `.jsonl` questions are stored. E.g., `dev.jsonl`, `test.jsonl`, `train.jsonl`
    in_question_paths = [
        os.path.join(original_wikisql_data_dir, f"{split}.jsonl") for split in splits_to_convert
    ]
    convert_all_questions(base_dir=base_out_dir, in_question_paths=in_question_paths)
    merge_wiki_sql_and_spider_schemas(base_dir=base_out_dir)
