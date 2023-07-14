# UNITE: A Unified Benchmark for Text-to-SQL Evaluation
This benchmark is composed of 18 publicly available text-to-SQL datasets, containing natural language questions from more than 12 domains, SQL queries from more than 3.9K patterns, and 29K databases. Compared to the widely used Spider benchmark, we introduce ∼120K additional examples and a threefold increase in SQL patterns, such as comparative and boolean questions. More details can be found in this [paper](https://arxiv.org/abs/2305.16265):
```
@misc{lan2023unite,
      title={UNITE: A Unified Benchmark for Text-to-SQL Evaluation}, 
      author={Wuwei Lan and Zhiguo Wang and Anuj Chauhan and Henghui Zhu and Alexander Li and Jiang Guo and Sheng Zhang and Chung-Wei Hang and Joseph Lilien and Yiqun Hu and Lin Pan and Mingwen Dong and Jun Wang and Jiarong Jiang and Stephen Ash and Vittorio Castelli and Patrick Ng and Bing Xiang},
      year={2023},
      eprint={2305.16265},
      archivePrefix={arXiv},
      primaryClass={cs.CL}
}
```


## Setup
This project uses [Poetry](https://python-poetry.org/docs/) for managing dependencies. For getting the appropriate python version you can look into [PyEnv](https://github.com/pyenv/pyenv). 

`poetry env use $(pyenv which python3)` 
can allow you to install a specific version using `pyenv` and letting poetry know to use it.

Clone the original datasets into the `original` folder. 

## Usage

There is one script per dataset in the `scripts` folder which published its output in the `unified` folder.

Usage example:
`poetry run python3 scripts/prepare_cosql.py`

## Data format for Unified Text2SQL.

1. **tables.jsonl** (Based off https://github.com/taoyds/spider/blob/master/README.md#tables)
    1. db_id
    2. table_names
    3. primary_keys
    4. foreign_keys
    5. column_names
        1. List of List (number of columns) of Tuples (table_number, column_name)
            1. Where table_number is the index of the table in table_names.
    6. column_types
        1. List of column_type:: text, number for the column in column_names by index.
2. **train.jsonl**
    1. db_id
    2. query
    3. question
3. **database/**
    1. {db_id}
        1. {db_id}.sqlite

## Spider-Syn:
The `tables.json` and `database/` was copied from `unified/spider`.

For train/dev files `SpiderSynQuestion` is used as the `question` while retaining the `query` and `db_id.


## DB: Patients
The dataset "_covers different linguistic variations for the user NL input and maps it to an expected SQL output._".

The dataset is split into the sub-datasets `unified/{flavour}_paraphrase_bench` which share the same `database/` and `tables.json`:

    1. Naıve: ”What is the average length of stay of patients where age is 80?”
    2. Syntactic: ”Where age is 80, what is the average length of stay of patients?”
    3. Morphological: ”What is the averaged length of stay of patients where age equaled 80?”
    4. Lexical: ”What is the mean length of stay of patients where age is 80 years?”
    5. Semantic: ”What is the average length of stay of patients older than 80?”
    6. Missing Information: ”What is the average stay of patients who are 80?”


## Stats:

Usage: `poetry run python3 stats/generate_statistics.py`

Generates schema level and NLQ level statistics using the [SQLglot](https://github.com/tobymao/sqlglot) parser.


## Evaluation:
Our evaluation metric is based on execution accuracy, please refer [spider test suite eval](https://github.com/taoyds/test-suite-sql-eval) and type command like the following for execution accuracy:
```
python evaluation.py --gold [gold file] --pred [predicted file] --etype exec --db [database dir]

arguments:
  [gold file]        gold.sql file where each line is `a gold SQL \t db_id`
  [predicted file]   predicted sql file where each line is a predicted SQL
  [evaluation type]  we only support "exec" evaluation.
  [database dir]     directory which contains sub-directories where each SQLite3 database is stored

```


## References:

1. Spider: https://github.com/taoyds/spider
1. WikiSQL: https://github.com/salesforce/WikiSQL
1. SQUALL: https://github.com/tzshi/squall
1. Spider-Syn: https://github.com/ygan/Spider-Syn
1. Criteria2SQL: https://github.com/xiaojingyu92/Criteria2SQL
1. SParC: https://github.com/taoyds/sparc
1. CoSQL: https://github.com/taoyds/cosql
1. Spider-DK: https://github.com/ygan/Spider-DK
1. ParaphraseBench: https://github.com/DataManagementLab/ParaphraseBench
1. XSP (Restaurants): https://github.com/google-research/language/tree/master/language/xsp
1. KaggleDBQA: https://www.microsoft.com/en-us/research/publication/kaggledbqa-realistic-evaluation-of-text-to-sql-parsers/
1. ACL-SQL: https://dl.acm.org/doi/10.1145/3430984.3431046
1. SEOSS-Queries: https://www.sciencedirect.com/science/article/pii/S2352340922004152
1. FIBEN: https://github.com/IBM/fiben-benchmark
1. SQLGlot: https://github.com/tobymao/sqlglot
