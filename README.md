# Text2SQL
[![Coverage Status](https://coveralls.io/repos/github/Conrad-X/text2SQL/badge.svg?branch=coverall_testing)](https://coveralls.io/github/Conrad-X/text2SQL?branch=coverall_testing)

A Python application that generates SQL queries from NLP questions on a given Schema. Currently set to run with Bird-SQL

## Table of Contents

1. [Project Overview](#project-overview)
2. [Project Structure](#project-structure)
3. [Setup Instructions](#setup-instructions)
4. [Preprocessing](#preprocessing)
5. [Prediction](#prediction)
6. [Evaluation](#evaluation)

## Project Overview

This application leverages large language models (LLMs) to transform natural language questions into SQL queries. It uses SQLite as the database and integrates with multiple llm providers for query generation.

## Project Structure

```bash
text2SQL
├── README.md                             # Project documentation and setup instructions                  
└── server
    ├── requirements.txt                  # List of python dependencies for the project         
    ├── app
    │   └── db.py                         # Database connections 
    ├── bird_eval                         # Evaluation scripts for BIRD dataset
    │   ├── evaluation.py                 # Main evaluation script
    │   ├── evaluation_ves.py             # Evaluation script for VES variant
    │   └── run                           # This folder contains bash scripts to run evaluation scripts
    │       ├── run_evaluation.sh         # Run execution accuracy evaluation
    │       └── run_evaluation_ves.sh     # Run VES evaluation
    ├── data                              # Data storage directory
    │   └── bird                          # BIRD dataset files
    │       └──
    ├── predict_sqls                      # SQL prediction scripts
    │   ├── predict_sqls.py               # Main SQL prediction logic
    │   └── run                           # Contains shell script to run main prediction script
    │       └── run_predictions.sh        # Run SQL prediction
    ├── preprocess                              # Data preprocessing scripts
    │   ├── add_descriptions_bird_dataset.py    # Adds descriptions to the BIRD dataset
    │   ├── add_runtime_pruned_schema.py        # Add pruned schema to the test set
    │   ├── prepare_sample_dataset.py           # Prepares a sample dataset for testing
    │   └── run                                 # Contains shell scritps to run preprocessing scripts
    │       ├── run_preprocess_test.sh          # Run preprocessing on test set
    │       └── run_preprocess_train.sh         # Run preprocessing on train set
    ├── services                           # API client services for various LLMs
    │   ├── anthropic_client.py            # Client for Anthropic AI
    │   ├── base_client.py                 # Base class for AI service clients
    │   ├── client_factory.py              # Factory to instantiate different clients
    │   ├── dashscope_client.py            # Client for DashScope API
    │   ├── deepseek_client.py             # Client for DeepSeek API
    │   ├── google_ai_client.py            # Client for Google AI services
    │   └── openai_client.py               # Client for OpenAI API
    └── utilities                          # Utility scripts for various functionalities
        ├── candidate_selection.py          # Selects candidate SQL queries
        ├── config.py                       # Configuration settings
        ├── constants                       # Constants directory
        │    └──
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

The server/data folder consists of the dev and train datasets, with the `processed_train.json` file:

```bash
server/data
├── bird
│   ├── dev_20240627
│   │   ├── dev_databases
│   │   ├── dev_gold.sql
│   │   ├── processed_test.json
│   │   ├── dev.json
│   │   ├── dev_tables.json
│   │   ├── column_meaning.json
│   │   └── dev_tied_append.json
│   ├── train
│   │   ├── train_databases
│   │   ├── train_gold.sql
│   │   ├── processed_train.json
│   │   ├── train.json
│   │   └── train_tables.json
│   └── test
│       ├── test_databases
│       ├── test_gold.sql
│       ├── processed_test.json *
│       ├── test.json
│       ├── column_meaning.json
│       └── test_tables.json



```
\*processed_test.json is created after preprocessing (explained later).

Download the test dataset and set `BIRD_TEST_DIR_PATH` in [server/.env](server/.env) to the test path. If the dataset path is outside the project directory make sure it is the absolute path. The default test directory is `./data/bird/test`. Ensure that the `column_meaning.json` file is in the root of the test directory and dev directory. To run predictions for the dev set ensure that the dev dataset has both `column_meaning.json` in the root as well as the database_descriptions in each database.

Ensure that the `DATASET_TYPE` and `SAMPLE_DATASET_TYPE` in [server/.env](server/.env) is set the `bird_test` and `bird_train`. If you want to run predictions on the dev set then set `DATASET_TYPE` to `bird_dev`. Here, `DATASET_TYPE` refers to the test set and `SAMPLE_DATASET_TYPE` refers to the questions path where we get our few shots from. There are 3 options that can be set here: `bird_dev`, `bird_test` and `bird_train`. 

Also set the correct dataset paths in `BIRD_TEST_DIR_PATH`. If the dataset path is outside the project directory make sure it is the absolute path.

If you download the dev dataset from scratch, rename `dev.sql` (the gold file) to `dev_gold.sql`. The default `evaluation.py` file appends `_gold` to the gold file.

Note: We have provided the train and dev set. The original train set had alot of errors, these included missing column names, spelling mistakes and extra spaces in csv files. We have fixed these issues in the provided train set. There were approximately 50 errors that we manually fixed. Notably we encountered the following errors:

- Column names in .csv files for descriptions did not match the sqlite column names.
- .csv file had extra spaces.
- .csv file names for tables were not consistent with sqlite table names.

#### .env File

We have provided the `.env` file with our submission. The `.env` file is located in the server directory. The `.env` file should follow the following format.

```
OPENAI_API_KEY =
ANTHROPIC_API_KEY =
GOOGLE_AI_API_KEY =
DEEPSEEK_API_KEY =
DASHSCOPE_API_KEY =


ALL_GOOGLE_API_KEYS =     #LIST OF ALL GOOGLE API KEYS SPACE SEPERATED *

ANONYMIZED_TELEMETRY = 
ALLOW_RESET = 
TOKENIZERS_PARALLELISM = 
GRPC_VERBOSITY =


DATASET_TYPE =
SAMPLE_DATASET_TYPE =

BIRD_TEST_DIR_PATH = TEST PATH HERE
BIRD_DEV_DIR_PATH = DEV PATH HERE
BIRD_TRAIN_DIR_PATH = TRAIN PATH HERE
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

\* `ALL_GOOGLE_API_KEYS` refers to a list of space seperated Google API Keys.

## Preprocessing

We preprocess the test dataset for schema pruning from the NLP question. We also update the default descriptions for tables and columns. By running `run_preprocess_test.sh` you will generate the `processed_test.json` file in the test folder which is used by our prediction pipeline. Run the following to preprocess the test dataset:

```sh
cd server/

chmod +x preprocess/run/run_preprocess_test.sh

./preprocess/run/run_preprocess_test.sh
```

Similar to the test dataset, we also preprocess the train dataset to add pruned schema from the gold SQLs. We also improve on the default descriptions for tables and columns. We generate a `processed_train.json` file that we use in our pipeline.

We have provided the `processed_train.json` file in the zipped code file so you don't need to preprocess the train dataset before hand.
If you need to preprocess the train dataset yourself then run the following:

```sh
chmod +x preprocess/run/run_preprocess_train.sh

./preprocess/run/run_preprocess_train.sh
```

**Important Note**: Keep in mind that the train dataset descriptions have a lot of errors. These include extra lines in csvs, spelling mistakes and missing columns. If you decide to run the preprocess dataset script with the original train set then you will need to fix these errors yourself. We have provided the fixed train dataset with our submission, THIS IS NOT THE ORIGINAL.

## Prediction

SQL prediction takes a significant amount of time: ~8 hours. We save the generated predictions as they are generated so the script can be restarted and it will continue where it left off. There is a non-zero chance that the script will get stuck so restart it if need be using Crtl + C. Similarly, if you encounter an error, restart the script and it will continue where it left off. 


To run NL to SQL prediction, run the following:

```sh
chmod +x predict_sqls/run/run_predictions.sh

./predict_sqls/run/run_predictions.sh
```

This will create a `predict_{dataset_type}.json` file in the corresponding dataset directory. For the test dataset, `predict_test.json` will be created in test dataset directory.

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

## Code Quality with Pre-commit Hooks

This project uses [`pre-commit`](https://pre-commit.com/) to enforce code quality, style, and best practices automatically before each commit.

### Included Tools

The following tools are planned for integration via `pre-commit`. Currently, only `isort` is active:

| Tool        | Purpose                                  | Enforced PEP     | Status    |
|-------------|-------------------------------------------|------------------|-----------|
| `isort`     | Sorts and organizes imports               | PEP 8 (Imports)  | Active    |
| `black`     | Auto-formats code to PEP 8 style          | PEP 8            | Inactive  |
| `flake8`    | Lints code for formatting and logic issues| PEP 8            | Inactive  |
| `mypy`      | Performs static type checking              | PEP 484, 561     | Inactive  |
| `pydocstyle`| Enforces consistent docstring formatting  | PEP 257          | Inactive  |

### Installation

1. Install `pre-commit`:
   ```bash
   pip install pre-commit
   ```

2. Set up pre-commit hooks to run automatically before each commit:
   ```bash
   pre-commit install
   pre-commit run --all-files
   pre-commit run --files <file1> <file2> ...
   ```