# Author: Anuj Chauhan

import os
import json
import sqlite3
from schema_generator import dump_db_json_schema

# Since the original db was in MySql,
# some aspects of it does not translate directly to sqlite.

# https://www2.sqlite.org/cvstrac/wiki?p=ConverterTools
sqlite_compatible_patients_dump = """
CREATE TABLE `patients` (
  `id` integer NOT NULL primary key,
  `first_name` varchar(255) default NULL,
  `last_name` varchar(255) default NULL,
  `diagnosis` varchar(255) default NULL,
  `length_of_stay` mediumint default NULL,
  `age` mediumint default NULL,
  `gender` varchar(255) default NULL
);

INSERT INTO `patients` (`first_name`,`last_name`,`diagnosis`,`length_of_stay`,`age`,`gender`) VALUES ("Baker","Harrington","heart disease",8,50,"female"),("Florence","Patterson","tuberculosis",8,94,"male"),("Sasha","Hoffman","liver disease",8,4,"other"),("Maya","Woods","liver disease",2,41,"male"),("Baker","Morris","tuberculosis",7,76,"other"),("Florence","Morris","stroke",2,53,"female"),("Bruce","Blake","stroke",13,45,"male"),("Tate","Patterson","tuberculosis",20,57,"other"),("Tara","Ford","allergies",16,52,"female"),("Maya","Patterson","flu",14,58,"female"),("Tate","Gibson","cancer",9,83,"female"),("Tara","Ford","asthma",2,23,"other"),("Bruce","Silva","heart disease",20,62,"other"),("Baker","Gibson","allergies",1,57,"male"),("Maya","Silva","hiv",15,9,"male"),("Florence","Ford","diabetes",7,75,"male"),("Bruce","Guerrero","tuberculosis",19,28,"male"),("Ian","Morris","flu",12,50,"other"),("Bruce","Hoffman","stroke",8,2,"female"),("Maya","Harrington","cancer",11,31,"female"),("Bruce","Gibson","allergies",1,87,"other"),("Tate","Guerrero","diarrhea",15,17,"other"),("Maya","Woods","stroke",3,98,"male"),("Tara","Patterson","allergies",9,79,"male"),("August","Hoffman","heart disease",18,59,"male"),("Baker","Morris","heart disease",3,48,"other"),("Ian","Ford","tuberculosis",20,57,"other"),("Bruce","Gibson","stroke",20,32,"other"),("Tate","Gibson","hiv",18,97,"male"),("Baker","Hoffman","heart disease",18,26,"other"),("Florence","Silva","hiv",16,6,"other"),("Baker","Harrington","asthma",17,18,"female"),("August","Patterson","stroke",10,94,"female"),("August","Silva","diabetes",2,90,"male"),("Bruce","Ford","cancer",16,97,"female"),("Baker","Gibson","stroke",3,32,"other"),("Sasha","Ford","diabetes",19,80,"male"),("August","Silva","allergies",2,57,"male"),("Sasha","Gibson","flu",8,19,"other"),("Tate","Morris","diabetes",13,82,"female"),("Mary","Morris","cancer",14,91,"other"),("Sasha","Silva","asthma",2,42,"female"),("Baker","Guerrero","flu",11,2,"male"),("Mary","Patterson","hiv",12,84,"male"),("Tate","Patterson","heart disease",4,37,"female"),("Tara","Patterson","cancer",15,57,"male"),("Florence","Patterson","cancer",18,83,"other"),("Sasha","Morris","stroke",15,11,"female"),("Tara","Woods","diarrhea",2,73,"female"),("Florence","Blake","cancer",13,30,"other"),("Sasha","Hoffman","asthma",2,67,"female"),("Sasha","Harrington","liver disease",19,95,"female"),("Tate","Silva","hiv",15,19,"other"),("Florence","Guerrero","asthma",8,16,"female"),("Florence","Silva","stroke",8,80,"male"),("Tate","Harrington","flu",2,29,"other"),("Baker","Hoffman","tuberculosis",9,69,"other"),("Mary","Guerrero","liver disease",18,69,"other"),("Mary","Harrington","diabetes",20,19,"male"),("Tate","Guerrero","hiv",11,89,"male"),("Maya","Hoffman","flu",16,28,"female"),("Sasha","Blake","hiv",20,49,"male"),("Maya","Patterson","cancer",7,41,"other"),("Tate","Blake","allergies",2,25,"male"),("Tara","Silva","stroke",3,89,"female"),("Bruce","Morris","diabetes",14,56,"other"),("Maya","Ford","tuberculosis",3,91,"female"),("Baker","Hoffman","diabetes",9,86,"other"),("Ian","Morris","heart disease",18,57,"other"),("Maya","Patterson","diarrhea",2,28,"other"),("August","Blake","diarrhea",20,16,"other"),("Sasha","Hoffman","diarrhea",6,57,"male"),("Tara","Harrington","stroke",9,92,"male"),("Mary","Morris","flu",12,41,"other"),("Sasha","Ford","liver disease",11,56,"male"),("Ian","Blake","heart disease",10,18,"male"),("Mary","Guerrero","cancer",10,74,"male"),("Florence","Morris","tuberculosis",7,12,"male"),("Florence","Guerrero","stroke",3,56,"other"),("August","Silva","heart disease",11,32,"other"),("Tara","Hoffman","heart disease",15,33,"other"),("Bruce","Hoffman","liver disease",16,80,"other"),("Bruce","Woods","cancer",2,14,"male"),("Florence","Ford","allergies",11,38,"other"),("Bruce","Gibson","cancer",8,98,"female"),("August","Blake","stroke",20,38,"female"),("Ian","Harrington","diabetes",2,98,"male"),("Mary","Blake","stroke",10,78,"other"),("Ian","Guerrero","flu",7,18,"female"),("Baker","Harrington","flu",5,95,"other"),("Baker","Ford","stroke",13,1,"male"),("Tate","Hoffman","liver disease",6,40,"female"),("Maya","Hoffman","hiv",1,18,"female"),("Bruce","Patterson","diabetes",20,5,"male"),("Florence","Blake","liver disease",14,72,"male"),("August","Guerrero","diarrhea",2,13,"male"),("Tate","Morris","tuberculosis",16,90,"male"),("Tara","Hoffman","allergies",2,77,"other"),("Tara","Harrington","tuberculosis",12,27,"female"),("Florence","Woods","heart disease",18,73,"other");
"""

UNIFIED_DB_DIR = "unified/"

NLQ_FLAVORS = [
    "naive",
    "syntactic",
    "morphological",
    "lexical",
    "semantic",
    "missing",
]

OG_INPUT_DIR = "original/ParaphraseBench/test"
og_gold_sqls = open(f"{OG_INPUT_DIR}/patients_test.sql").readlines()
og_gold_sqls = [sql.strip() for sql in og_gold_sqls]

db_id = "patients"

for flavour in NLQ_FLAVORS:
    flavour_db_dir = (
        UNIFIED_DB_DIR + flavour + "_paraphrase_bench" + "/database/" + db_id
    )
    os.makedirs(flavour_db_dir, exist_ok=True)

    db_path = f"{flavour_db_dir}/{db_id}.sqlite"
    if os.path.exists(db_path):
        os.unlink(db_path)

    conn = sqlite3.connect(db_path)
    conn.executescript(sqlite_compatible_patients_dump)

    tables = [dump_db_json_schema(db_path)]
    flavour_dir = UNIFIED_DB_DIR + flavour + "_paraphrase_bench/"

    with open(flavour_dir + "tables.json", "w") as f:
        json.dump(tables, f, indent=2)

    flavoured_questions = open(
        f"{OG_INPUT_DIR}/{flavour}_source.txt"
    ).readlines()
    flavoured_questions = [
        question.strip() for question in flavoured_questions
    ]

    flavoured_dev_file_path = f"{flavour_dir}/dev.jsonl"
    if os.path.exists(flavoured_dev_file_path):
        os.unlink(flavoured_dev_file_path)

    with open(flavoured_dev_file_path, "a+") as writer:
        for question, query in zip(flavoured_questions, og_gold_sqls):
            unified_json_entry = {}
            unified_json_entry["db_id"] = db_id
            unified_json_entry["question"] = question
            unified_json_entry["query"] = query

            json.dump(unified_json_entry, writer)
            writer.write("\n")
