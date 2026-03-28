#!/bin/bash

INPUT_DIR=$1
OUTPUT_DIR=$2

if [ -z "$INPUT_DIR" ] || [ -z "$OUTPUT_DIR" ]; then
  echo "Użycie: run_mr.sh <input_dir> <output_dir>"
  exit 1
fi

# usuń stare wyniki (jeśli istnieją)
hdfs dfs -rm -r -f $OUTPUT_DIR

hadoop jar /usr/lib/hadoop/hadoop-streaming.jar \
  -input $INPUT_DIR \
  -output $OUTPUT_DIR \
  -mapper mapper.py \
  -combiner combiner.py \
  -reducer reducer.py \
  -file mapper.py \
  -file combiner.py \
  -file reducer.py
