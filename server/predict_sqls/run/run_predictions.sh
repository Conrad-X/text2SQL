#!/bin/bash
export PYTHONPATH=$(pwd):$PYTHONPATH

echo '''Running Predictions for BIRD Dataset'''
python3 -u ./predict_sqls/predict_sqls.py