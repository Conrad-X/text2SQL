#!/bin/bash
export PYTHONPATH=$(pwd):$PYTHONPATH

echo '''Preparing Sample Dataset'''
python3 -u ./preprocess/prepare_sample_dataset.py
