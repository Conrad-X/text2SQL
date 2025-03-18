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
├── README.md                             # Project documentation and setup instructions
├── requirements.txt                      # List of python dependencies for the project
└── server                                # Main server directory
    ├── app
    │   └── db.py                         # Database connections 
    ├── bird_eval                         # Evaluation scripts for BIRD dataset
    │   ├── evaluation.py                 # Main evaluation script
    │   ├── evaluation_ves.py             # Evaluation script for VES variant
    │   └── run                           # This folder contains bash scripts to run evaluation scripts
    │       └──
    ├── data                              # Data storage directory
    │   └── bird                          # BIRD dataset files
    │       └──
    ├── predict_sqls                      # SQL prediction scripts
    │   ├── predict_sqls.py               # Main SQL prediction logic
    │   └── run                           # Contains shell script to run main prediction script
    │       └──
    ├── preprocess                              # Data preprocessing scripts
    │   ├── add_descriptions_bird_dataset.py    # Adds descriptions to the BIRD dataset
    │   ├── add_runtime_pruned_schema.py        # Add pruned schema to the test set
    │   ├── prepare_sample_dataset.py           # Prepares a sample dataset for testing
    │   └── run                                 # Contains shell scritps to run preprocessing scripts
    │       └──
    ├── services                           # API client services for various LLMs
    │   ├── anthropic_client.py            # Client for Anthropic AI
    │   ├── base_client.py                 # Base class for AI service clients
    │   ├── client_factory.py              # Factory to instantiate different clients
    │   ├── dashscope_client.py            # Client for DashScope API
    │   ├── deepseek_client.py             # Client for DeepSeek API
    │   ├── google_ai_client.py            # Client for Google AI services
    │   └── openai_client.py               # Client for OpenAI API
    └── utilities                          # Utility scripts for various functionalities
        ├── batch_job.py                    # Handles batch processing tasks
        ├── candidate_selection.py          # Selects candidate SQL queries
        ├── config.py                       # Configuration settings
        ├── constants                       # Constants directory
        │    └──
        ├── cost_estimation.py              # Estimates the cost of queries
        ├── generate_schema_used.py         # Generates Schema used from SQL queries
        ├── logging_utils.py                # Logging utility functions
        ├── m_schema                        # Directory that handles M-Schema Generation 
        │    └──
        ├── path_config.py                  # Manages file paths
        ├── prompts                         # Directory with Prompt Templates and configs
        │    └──
        ├── schema_linking                  # Directory that handles schema linking logic
        │    └──
        ├── sql_improvement.py              # Refines and optimizes SQL queries
        ├── utility_functions.py            # Miscellaneous utility functions
        └── vectorize.py                    # Handles Vectorization
     
```

## Setup Instructions

### Setting up the Dataset

All of the Bird dataset should be within the `server/data/bird` folder. In the case of [dev](https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip) and [train](https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip) download the respective datasets and save them in the `data/bird` folder like so:

```bash
server/data
├── bird
│   ├── dev_20240627
│   │   ├── dev_databases
│   │   ├── dev_gold.sql
│   │   ├── dev_tables.json
│   │   └── dev_tied_append.json
│   └── train
│       ├── processed_train.json
│       ├── train.json
│       ├── train_databases
│       ├── train_gold.sql
│       └── train_tables.json

```

Download the test dataset in [server/data/bird](server/data/bird/) and set `BIRD_TEST_DIR_PATH` in [server/.env](server/.env) to the test path.

Ensure that the `DATASET_TYPE` and `SAMPLE_DATASET_TYPE` in [server/.env](server/.env) is set the `bird_test` and `bird_train`. If the dataset path is outside the project directory make sure it is the absolute path.

Note: We have provided the train and dev set. The original train set had alot of errors, these included missing column names, spelling mistakes and extra spaces in csv files. We have fixed these issues in the provided train set.

#### .ENV File

Place the .env file in the project root. The .env file should follow the following format.

```
OPENAI_API_KEY=
ANTHROPIC_API_KEY=
GOOGLE_AI_API_KEY=
DEEPSEEK_API_KEY=
DASHSCOPE_API_KEY=


ALL_GOOGLE_API_KEYS=     #LIST OF ALL GOOGLE API KEYS SPACE SEPERATED

ANONYMIZED_TELEMETRY=False
ALLOW_RESET=TRUE
TOKENIZERS_PARALLELISM = FALSE
GRPC_VERBOSITY=NONE


DATASET_TYPE=bird_test
SAMPLE_DATASET_TYPE=bird_train

BIRD_TEST_DIR_PATH= TEST PATH HERE
BIRD_DEV_DIR_PATH="./data/bird/dev_20240627"
BIRD_TRAIN_DIR_PATH="./data/bird/train"
```

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

We preprocess the test dataset for schema pruning. Run the following commands to preprocess the data:

```sh
cd server/

chmod +x preprocess/run/run_preprocess_test.sh

./preprocess/run/run_preprocess_test.sh
```

We have provided the processed_train.json file so you don't need to preprocess the train dataset before hand. If however you want to generat this file you can run the following:

Note: Keep in mind that the train dataset descriptions have a lot of errors. These include extra lines in csvs, spelling mistakes and missing columns. If you decide to run the preprocess dataset script with the original train set then you will need to fix these errors yourself. We have provided the fixed train dataset with our submission, THIS IS NOT THE ORIGINAL.

```sh
chmod +x preprocess/run/run_preprocess_train.sh

./preprocess/run/run_preprocess_train.sh
```

## Prediction

To run NLP to SQL prediction, run the following:

```sh
chmod +x predict_sqls/run/run_predictions.sh
./predict_sqls/run/run_predictions.sh
```

This will create a `predict_test.json` file in `server/data/bird/test/`.

## Evaluation

To evaluate the generated SQLs, we included the original evaluation scripts in [server/bird_eval/](server/bird_eval/). To run the scripts you can use shell scripts [run_evaluation.sh](server/bird_eval/run/run_evaluation.sh) and [run_evaluation_ves.sh](server/bird_eval/run/run_evaluation_ves.sh). You can run them as follows:

```sh
chmod +x bird_eval/run/run_evaluation.sh
chmod +x bird_eval/run/run_evaluation_ves.sh

./bird_eval/run/run_evaluation.sh
./bird_eval/run/run_evaluation_ves.sh
```

Before running the scripts ensure that the paths for the predicted SQLS and base datset directory is correctly set in [run_evaluation.sh](server/bird_eval/run/run_evaluation.sh) and [run_evaluation_ves.sh](server/bird_eval/run/run_evaluation_ves.sh).

Note: There is a small bug fix in both the scripts. The bug is in the line 13 of [run_evaluation.sh](server/bird_eval/run/run_evaluation.sh) and [run_evaluation_ves.sh](server/bird_eval/run/run_evaluation_ves.sh) explained in the docstring of the scripts.

# Note:
Once you run the predict script we save the generated SQLs as they are generated. If for some reason the script gets stuck you can restart the scripts and it will continue from where it left off. 
