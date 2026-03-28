#!/bin/bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Użycie: $0 <input_dir3> <input_dir4> <output_dir6>"
  exit 1
fi

INPUT_DIR3=$1
INPUT_DIR4=$2
OUTPUT_DIR6=$3

: ${BEELINE_URL:="jdbc:hive2://localhost:10000/default"}

echo "Uruchamiam beeline z URL: $BEELINE_URL"
echo "Parametry: input_dir3=$INPUT_DIR3, input_dir4=$INPUT_DIR4, output_dir6=$OUTPUT_DIR6"

beeline \
  -u "$BEELINE_URL" \
  --silent=true --outputformat=csv2 \
  --hiveconf input_dir3="$INPUT_DIR3" \
  --hiveconf input_dir4="$INPUT_DIR4" \
  --hiveconf output_dir6="$OUTPUT_DIR6" \
  --hiveconf fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --hiveconf fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  -f hive.hql

EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  echo "Beeline zakończył się pomyślnie."
else
  echo "Beeline zakończył się z kodem: $EXIT_CODE" >&2
fi

exit $EXIT_CODE
