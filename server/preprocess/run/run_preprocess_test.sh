#!/bin/bash
export PYTHONPATH=$(pwd):$PYTHONPATH

echo '''Adding descriptions to Testing Dataset'''
python3 -u ./preprocess/add_descriptions_bird_dataset.py

echo '''Adding runtime pruned schema to Testing items'''
python3 -u ./preprocess/add_runtime_pruned_schema.py