# Author: Jun Wang
"""
This script is about how to convert csv file to sqlite database, and then build Spider dataset styple table.json
Originial used to convert data here to spider format: https://github.com/rohitshantarampatil/sql-nlp
"""

import os
import csv
import json
import sqlite3
import functools

from schema_generator import dump_db_json_schema


def quote_str(s):
    if len(s) == 0:
        return "''"
    if len(s) == 1:
        if s == "'":
            return "''''"
        else:
            return "'%s'" % s
    if s[0] != "'" or s[-1:] != "'":
        return "'%s'" % s.replace("'", "''")
    return s


def quote_list(l):
    return [quote_str(x) for x in l]


def quote_list_as_str(l):
    return ",".join(quote_list(l))


def read_csv_to_db(sqldb, infilename, table_name):
    dialect = csv.Sniffer().sniff(open(infilename, "rt").readline())
    inf = csv.reader(open(infilename, "rt"), dialect)

    column_names = next(inf)

    if column_names[0] == "":
        column_names[0] = "idx"

    colstr = ",".join(column_names)

    try:
        sqldb.execute("drop table %s;" % table_name)
    except:
        pass

    try:
        sqldb.execute("create table %s (%s);" % (table_name, colstr))
    except:
        print(f"Could not create table {table_name}. Supplied columns: {colstr}")
        return

    try:
        for l in inf:
            sql = "insert into %s values (%s);" % (table_name, quote_list_as_str(l))
            sqldb.execute(sql)
    except:
        print(f"Could not insert values in {table_name}. Value list: {l}")
        pass

    sqldb.commit()


def csvfiles_to_sqlite_db(original_csv_files, db_file):
    conn = sqlite3.connect(db_file)
    for csvfile in original_csv_files:
        inhead, intail = os.path.split(csvfile)
        tablename = os.path.splitext(intail)[0]
        read_csv_to_db(conn, csvfile, tablename)
    print(f"CSV files dumped into sqlite databases.")


def extract_sqls_from_db_file(input_db_file, output_sql_file):
    con = sqlite3.connect(input_db_file)
    sql_lines = []
    for line in con.iterdump():
        sql_lines.append(line)

    with open(output_sql_file, "w") as fout:
        fout.write("\n".join(sql_lines))


def pp(cursor):
    rows = cursor.fetchall()
    desc = cursor.description
    if not desc:
        return rows
    names = [d[0] for d in desc]
    rcols = range(len(desc))
    rrows = range(len(rows))
    maxen = [max(0, len(names[j]), *(len(str(rows[i][j])) for i in rrows)) for j in rcols]
    names = " " + " | ".join([names[j].ljust(maxen[j]) for j in rcols])
    sep = "=" * (functools.reduce(lambda x, y: x + y, maxen) + 3 * len(desc) - 1)
    rows = [names, sep] + [
        " " + " | ".join([str(rows[i][j]).ljust(maxen[j]) for j in rcols]) for i in rrows
    ]
    return "\n".join(rows) + (len(rows) == 2 and "\n no row selected\n" or "\n")


def check_executable(sqldb, sql_cmd):
    """Run a SQL command on the specified (open) sqlite3 database, and write out the output."""

    curs = sqldb.cursor()
    try:
        curs.execute(sql_cmd)
        return True
    except:
        return False


def extract_nlq_sql_pair(input_file, output_dir, db_file=None):
    csv_reader = csv.reader(open(input_file, "rt"))
    column_names = next(csv_reader)

    if db_file == None:
        conn = sqlite3.connect(":memory:")
    else:
        conn = sqlite3.connect(db_file)

    outputs = []
    for l in csv_reader:
        if len(l) == 2:
            query = l[1]
            line = dict(query=l[1], question=l[0], db_id="schema")
        else:
            query = l[2]
            line = dict(id=l[0], query=l[2], question=l[1], db_id="schema")
        if check_executable(conn, query):
            outputs.append(json.dumps(line))
        else:
            print(f"Failed query: {query}")
    return outputs


def get_tables_from_csv(input_dir, output_table_path):
    idx = 0
    column_names = []
    for file in os.listdir(input_dir):
        if file.endswith(".csv"):
            csv_file = os.path.join(input_dir, file)
            csv_reader = csv.reader(open(csv_file, "rt"))
            curr_column_names = next(csv_reader)
            column_names.extend([(idx, x) for x in curr_column_names])
            idx += 1
    column_types = ["text"] * len(column_names)
    db_id = "schema"
    contents = dict(column_names=column_names, column_types=column_types, db_id=db_id)
    with open(output_table_path, "w") as fout:
        json.dump(contents, fout)


def get_tables_from_db(db_file, output_table_path):
    schema_data = dump_db_json_schema(db_file, "schema")
    with open(output_table_path, "w") as fout:
        json.dump(schema_data, fout)


if __name__ == "__main__":
    original_csv_files = []
    original_csv_dir = "original/sql-nlp/Dataset"
    original_processed_dir = "original/sql-nlp/Final_Processed"

    output_data_dir = "unified/acl_sql"
    db_dir = f"{output_data_dir}/database"
    os.makedirs(f"{db_dir}/schema", exist_ok=True)
    os.makedirs(output_data_dir, exist_ok=True)

    for file in os.listdir(original_csv_dir):
        if file.endswith(".csv"):
            original_csv_files.append(os.path.join(original_csv_dir, file))

    db_file = os.path.join(db_dir, "schema", "schema.sqlite")
    sql_file = os.path.join(db_dir, "schema", "schema.sql")

    csvfiles_to_sqlite_db(original_csv_files, db_file=db_file)
    extract_sqls_from_db_file(input_db_file=db_file, output_sql_file=sql_file)

    tables = []
    tables.append(dump_db_json_schema(db_file))
    json.dump(tables, open(os.path.join(output_data_dir, "tables.json"), "w"), indent=2)

    data_files = []
    for file in os.listdir(original_processed_dir):
        if file.endswith(".csv"):
            data_files.append(os.path.join(original_processed_dir, file))
    outputs = []
    for data_file in data_files:
        outputs.extend(extract_nlq_sql_pair(data_file, output_data_dir, db_file))

    with open(os.path.join(output_data_dir, "test.jsonl"), "w") as fout:
        fout.write("\n".join(outputs))

    print(f"Conversion attempt complete!")
