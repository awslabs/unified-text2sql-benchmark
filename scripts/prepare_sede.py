# Author: Joe Lilien

import json
import sqlite3
import os
import xml.etree.cElementTree as etree
import logging

########################################################################################################################
# Before running this script, execute the following steps:
#
# * Download git package to local directory:
#    mkdir ./originalÅ
#    cd ./original
#    git clone https://github.com/hirupert/sede.git
#
# * WARNING: The data dump is _very_ large, and requires ~500GB of space to store all zipped files, unzipped copies, and
#            the final *.sqlite file
#
# * Download raw data dump from https://archive.org/download/stackexchange_20210301
#   * This is the 03/01/2021 SEDE archive data
#   * Download the following ".7z" archive files:
#     * stackoverflow.com-Badges.7z           280.5M
#     * stackoverflow.com-Comments.7z         4.6G
#     * stackoverflow.com-PostHistory.7z      28.6G
#     * stackoverflow.com-PostLinks.7z        98.1M
#     * stackoverflow.com-Posts.7z            16.2G
#     * stackoverflow.com-Tags.7z             851.2K
#     * stackoverflow.com-Users.7z            680.8M
#     * stackoverflow.com-Votes.7z            1.2G
#
# NOTE: The data archive only contains a subset of (the most significant) tables from the SEDE dataset. To address this,
# we exclude any SQL questions which reference a table not captured in one of the archive files (listed above^), and
# update our schema file accordingly.
# Some additional info:
# * Describing excluded tables in archive: https://tiny.amazon.com/5mst32vu/excludedtables
# * Full schema description (included/excluded list): https://tiny.amazon.com/r0q60myj/fullschema
#
# * Unzip the files using a 7z extractor
#
# * Run this script to prepare the data -> "python3 -m scripts.prepare_sede"
#   * Format `table.json` schema file
#   * Write out individual train/val/test.jsonl files
#   * Convert data dump XML files to single sqlite file (code adapted from https://meta.stackexchange.com/a/286488)
########################################################################################################################

UNIFIED_DB_DIR = "./unified/sede"

SCHEMA_FILE_PATH = "./original/sede/stackexchange_schema/tables_so.json"
EXAMPLES_DIR = "./original/sede/data/sede"

DATA_DUMP_DIR = "./original/sede/data/dump"

SPLITS = ["train", "val", "test"]

EXCLUDED_ORIGINAL_TABLES = [
    "CloseAsOffTopicReasonTypes",
    "CloseReasonTypes",
    "FlagTypes",
    "PendingFlags",
    "PostFeedback",
    "PostHistoryTypes",
    "PostNotices",
    "PostNoticeTypes",
    "PostsWithDeleted",
    "PostTags",
    "PostTypes",
    "ReviewRejectionReasons",
    "ReviewTaskResults",
    "ReviewTaskResultTypes",
    "ReviewTasks",
    "ReviewTaskStates",
    "ReviewTaskTypes",
    "SuggestedEdits",
    "SuggestedEditVotes",
    "TagSynonyms",
    "VoteTypes",
]


def main():
    # Format table schema file
    with open(SCHEMA_FILE_PATH) as s:
        schema_json = json.load(s)[0]

    tables_dict = {k: [] for k in schema_json.keys()}
    tables_dict["db_id"] = "sede"

    included_table_idx = -1
    prev_table_idx = -1
    for column_idx, column_info in enumerate(schema_json["column_names"]):
        table_idx = column_info[0]
        if table_idx == -1:
            tables_dict["column_names"].append(column_info)
            tables_dict["column_names_original"].append(column_info)
        elif schema_json["table_names"][table_idx].lower() in ANATHOMY.keys():
            if table_idx != prev_table_idx:
                included_table_idx += 1
                prev_table_idx = table_idx
                tables_dict["table_names"].append(
                    schema_json["table_names"][table_idx]
                )
                tables_dict["table_names_original"].append(
                    schema_json["table_names_original"][table_idx]
                )
            column_info[0] = included_table_idx
            tables_dict["column_names"].append(column_info)
            column_info_orignal = schema_json["column_names_original"][
                column_idx
            ]
            column_info_orignal[0] = included_table_idx
            tables_dict["column_names_original"].append(column_info_orignal)
            tables_dict["column_types"].append(
                schema_json["column_types"][column_idx]
            )

    with open(f"{UNIFIED_DB_DIR}/tables.json", "w+") as t:
        json.dump([tables_dict], t, indent=1)
    print(f"Successfully wrote out {UNIFIED_DB_DIR}/tables.json file!")

    # Format train/val/test examples
    total_dropped_count = 0
    total_examples_count = 0
    for split in SPLITS:
        with open(f"{EXAMPLES_DIR}/{split}.jsonl") as exs, open(
            f"{UNIFIED_DB_DIR}/{split}.jsonl", "w+"
        ) as exs_out:
            dropped_count = 0
            examples_count = 0
            for line in list(exs):
                examples_count += 1
                ex = json.loads(line)
                if any(
                    excluded_table.lower() in ex["QueryBody"].lower()
                    for excluded_table in EXCLUDED_ORIGINAL_TABLES
                ):
                    dropped_count += 1
                    continue
                formatted_ex = {
                    "db_id": tables_dict["db_id"],
                    "question": ex["Title"]
                    + (
                        f" {str(ex['Description'])}"
                        if ex["Description"]
                        else ""
                    ),
                    "query": ex["QueryBody"],
                }
                json.dump(formatted_ex, exs_out)
                exs_out.write("\n")
            print(
                f"Successfully wrote out {UNIFIED_DB_DIR}/{split}.jsonl file! Dropped {dropped_count}/{examples_count} examples."
            )
            total_dropped_count += dropped_count
            total_examples_count += examples_count
    print(
        f"Dropped {total_dropped_count}/{total_examples_count} examples in total"
    )

    # Convert XML data dump files to single .sqlite file
    print("Converting XML data dump files to single sede.sqlite file")
    dump_files(ANATHOMY.keys(), ANATHOMY)


ANATHOMY = {
    "Badges": {
        "Id": "INTEGER",
        "UserId": "INTEGER",
        "Name": "TEXT",
        "Date": "DATETIME",
        "Class": "INTEGER",
        "TagBased": "BOOLEAN",
    },
    "Comments": {
        "Id": "INTEGER",
        "PostId": "INTEGER",
        "Score": "INTEGER",
        "Text": "TEXT",
        "CreationDate": "DATETIME",
        "UserDisplayName": "TEXT",
        "UserId": "INTEGER",
        "ContentLicense": "TEXT",
    },
    "Posts": {
        "Id": "INTEGER",
        "PostTypeId": "INTEGER",  # 1: Question, 2: Answer
        "AcceptedAnswerId": "INTEGER",  # (only present if PostTypeId is 1)
        "ParentId": "INTEGER",  # (only present if PostTypeId is 2)
        "CreationDate": "DATETIME",
        "DeletionDate": "DATETIME",
        "Score": "INTEGER",
        "ViewCount": "INTEGER",
        "Body": "TEXT",
        "OwnerUserId": "INTEGER",  # (present only if user has not been deleted)
        "OwnerDisplayName": "TEXT",
        "LastEditorUserId": "INTEGER",
        "LastEditorDisplayName": "TEXT",  # ="Rich B"
        "LastEditDate": "DATETIME",  # ="2009-03-05T22:28:34.823"
        "LastActivityDate": "DATETIME",  # ="2009-03-11T12:51:01.480"
        "Title": "TEXT",
        "Tags": "TEXT",
        "AnswerCount": "INTEGER",
        "CommentCount": "INTEGER",
        "FavoriteCount": "INTEGER",
        "ClosedDate": "DATETIME",
        "CommunityOwnedDate": "DATETIME",  # (present only if post is community wikied)
        "ContentLicense": "TEXT",
    },
    "Votes": {
        "Id": "INTEGER",
        "PostId": "INTEGER",
        "VoteTypeId": "INTEGER",
        "UserId": "INTEGER",
        # -   1: AcceptedByOriginator
        # -   2: UpMod
        # -   3: DownMod
        # -   4: Offensive
        # -   5: Favorite
        # -   6: Close
        # -   7: Reopen
        # -   8: BountyStart
        # -   9: BountyClose
        # -  10: Deletion
        # -  11: Undeletion
        # -  12: Spam
        # -  13: InformModerator
        "CreationDate": "DATETIME",
        "BountyAmount": "INTEGER",
    },
    "PostHistory": {
        "Id": "INTEGER",
        "PostHistoryTypeId": "INTEGER",
        "PostId": "INTEGER",
        "RevisionGUID": "TEXT",
        "CreationDate": "DATETIME",
        "UserId": "INTEGER",
        "UserDisplayName": "TEXT",
        "Comment": "TEXT",
        "Text": "TEXT",
        "ContentLicense": "TEXT",
    },
    "PostLinks": {
        "Id": "INTEGER",
        "CreationDate": "DATETIME",
        "PostId": "INTEGER",
        "RelatedPostId": "INTEGER",
        "LinkTypeId": "INTEGER",
    },
    "Users": {
        "Id": "INTEGER",
        "Reputation": "INTEGER",
        "CreationDate": "DATETIME",
        "DisplayName": "TEXT",
        "LastAccessDate": "DATETIME",
        "WebsiteUrl": "TEXT",
        "Location": "TEXT",
        "AboutMe": "TEXT",
        "Views": "INTEGER",
        "UpVotes": "INTEGER",
        "DownVotes": "INTEGER",
        "ProfileImageUrl": "TEXT",
        "EmailHash": "TEXT",
        "AccountId": "INTEGER",
    },
    "Tags": {
        "Id": "INTEGER",
        "TagName": "TEXT",
        "Count": "INTEGER",
        "ExcerptPostId": "INTEGER",
        "WikiPostId": "INTEGER",
    },
}


def dump_files(
    file_names,
    anathomy,
    dump_path=DATA_DUMP_DIR,
    dump_database_name="sede.sqlite",
    create_query="CREATE TABLE IF NOT EXISTS {table} ({fields})",
    insert_query="INSERT INTO {table} ({columns}) VALUES ({values})",
    log_filename="so-parser.log",
):
    logging.basicConfig(
        filename=os.path.join(dump_path, log_filename), level=logging.INFO
    )
    db = sqlite3.connect(os.path.join(dump_path, dump_database_name))
    for file in file_names:
        print("Opening {0}.xml".format(file))
        with open(os.path.join(dump_path, file + ".xml")) as xml_file:
            tree = etree.iterparse(xml_file)
            # table_name = file.lower()
            table_name = file

            sql_create = create_query.format(
                table=table_name,
                fields=", ".join(
                    [
                        "{0} {1}".format(name, type)
                        for name, type in anathomy[table_name].items()
                    ]
                ),
            )
            print("Creating table {0}".format(table_name))

            try:
                logging.info(sql_create)
                db.execute(sql_create)
            except Exception as e:
                logging.warning(e)

            count = 0
            for events, row in tree:
                try:
                    if row.attrib.values():
                        logging.debug(row.attrib.keys())
                        query = insert_query.format(
                            table=table_name,
                            columns=", ".join(row.attrib.keys()),
                            values=("?, " * len(row.attrib.keys()))[:-2],
                        )
                        vals = []
                        for key, val in row.attrib.items():
                            if anathomy[table_name][key] == "INTEGER":
                                vals.append(int(val))
                            elif anathomy[table_name][key] == "BOOLEAN":
                                vals.append(1 if val == "TRUE" else 0)
                            else:
                                vals.append(val)
                        db.execute(query, vals)

                        count += 1
                        if count % 1000 == 0:
                            print("{}".format(count))

                except Exception as e:
                    logging.warning(e)
                    print("x", end="")
                finally:
                    row.clear()
            print("\n")
            db.commit()
            del tree


if __name__ == "__main__":
    main()
