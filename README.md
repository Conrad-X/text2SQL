# Text2SQL

A Python application that generates SQL queries from NLP questions on a given Schema. Currently set to run with Bird-SQL

## Table of Contents

1. [Project Overview](#project-overview)
2. [Project Structure](#project-structure)
3. [Setup Instructions](#setup-instructions)
4. [Preprocessing](#preprocessing)
5. [Prediction](#prediction)
6. [Evaluation](#evaluation)

## Project Overview

This application leverages large language models (LLMs) to transform natural language questions into SQL queries. It uses SQLite as the database and integrates with OpenAI, Anthropic, Gemini and Qwen for query generation.

## Project Structure

```bash
text2SQL
├── README.md
├── requirements.txt
├── docs
│   └── pull_request_template.md
└── server
    ├── app
    │   └── db.py
    ├── bird_eval
    │   ├── evaluation.py
    │   ├── evaluation_ves.py
    │   └── run
    ├── data
    │   └── bird
    ├── predict_sqls
    │   ├── predict_sqls.py
    │   └── run
    ├── preprocess
    │   ├── add_descriptions_bird_dataset.py
    │   ├── add_runtime_pruned_schema.py
    │   ├── prepare_sample_dataset.py
    │   └── run
    ├── requirements.txt
    ├── services
    │   ├── anthropic_client.py
    │   ├── base_client.py
    │   ├── client_factory.py
    │   ├── dashscope_client.py
    │   ├── deepseek_client.py
    │   ├── google_ai_client.py
    │   └── openai_client.py
    └── utilities
        ├── __init__.py
        ├── batch_job.py
        ├── candidate_selection.py
        ├── config.py
        ├── constants
        ├── cost_estimation.py
        ├── generate_schema_used.py
        ├── logging_utils.py
        ├── m_schema
        ├── path_config.py
        ├── prompts
        ├── schema_linking
        ├── sql_improvement.py
        ├── utility_functions.py
        └── vectorize.py           
```

## Setup Instructions

### Setting up the Dataset

All of the Bird dataset should be within the `server/data/bird` folder. In the case of [dev](https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip) and [train](https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip) download the respective datasets and save them in the `data/bird` folder like so:

```bash
server/data/
└── bird
    ├── dev_20240627
    │   ├── dev.json
    │   ├── dev_databases
    │   ├── dev_gold.sql
    │   ├── dev_tables.json
    │   └── predict_dev.json
    └── train
        ├── train.json
        ├── train_databases
        ├── train_gold.sql
        └── train_tables.json
```

Download the test dataset in [server/data/bird](server/data/bird/) and set `BIRD_TEST_DIR_PATH` in [server/.env](server/.env) to the test path.

Ensure that the `DATASET_TYPE` and `SAMPLE_DATASET_TYPE` in [server/.env](server/.env) is set the `bird_test` and `bird_train`. 

### Create a Virtual Environment

```sh
python -m venv venv
source venv/bin/activate
```

### Installing Dependancies

Install all required dependancies by running

```sh
pip install -r requirements.txt
```

## Preprocessing

We preprocess the train and test dataset for schema pruning. Run the following commands to preprocess the data:

```sh
cd server/

chmod +x preprocess/run/run_preprocess_train.sh
chmod +x preprocess/run/run_preprocess_test.sh

./preprocess/run/run_preprocess_train.sh
./preprocess/run/run_preprocess_test.sh
```

## Prediction

To run NLP to SQL prediction, run the following:

```sh
chmod +x predict_sqls/run/run_predictions.sh
./predict_sqls/run/run_predictions.sh
```

This will create a `predict_test.json` file in `server/data/bird/test/`. 

## Evaluation

To evaluate the generated SQLs you can we included the original evaluation scripts in [server/bird_eval/](server/bird_eval/). To run the scripts you can use shell scripts [run_evaluation.sh](server/bird_eval/run/run_evaluation.sh) and [run_evaluation_ves.sh](server/bird_eval/run/run_evaluation_ves.sh). You can run them as follows:

```sh
chmod +x bird_eval/run/run_evaluation.sh
chmod +x bird_eval/run/run_evaluation_ves.sh

./bird_eval/run/run_evaluation.sh
./bird_eval/run/run_evaluation_ves.sh
```

Before running the scripts ensure that the paths for the predicted SQLS and base datset directory is correctly set in [run_evaluation.sh](server/bird_eval/run/run_evaluation.sh) and [run_evaluation_ves.sh](server/bird_eval/run/run_evaluation_ves.sh).

Note: There is a small bug fix in both the scripts. The bug is in the line 13 of [run_evaluation.sh](server/bird_eval/run/run_evaluation.sh) and [run_evaluation_ves.sh](server/bird_eval/run/run_evaluation_ves.sh) explained in the docstring of the scripts.
