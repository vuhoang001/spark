#!/usr/bin/env bash
set -euo pipefail

# Move Olist CSV files from ../kafka-flink/data to this repo's data/olist/
# Usage: ./scripts/move_olist_data.sh

SRC_DIR="$(pwd)/../kafka-flink/data"
DST_DIR="$(pwd)/data/olist"

echo "Source: $SRC_DIR"
echo "Destination: $DST_DIR"

if [ -d "$DST_DIR" ]; then
  echo "Destination exists. Files will be merged into $DST_DIR"
else
  mkdir -p "$DST_DIR"
  echo "Created destination $DST_DIR"
fi

if [ -d "$SRC_DIR" ]; then
  echo "Found source directory. Looking for CSV files..."
  shopt -s nullglob
  count=0
  for f in "$SRC_DIR"/*.csv "$SRC_DIR"/olist/*.csv; do
    if [ -f "$f" ]; then
      cp -v "$f" "$DST_DIR/"
      count=$((count+1))
    fi
  done
  if [ $count -eq 0 ]; then
    echo "No CSV files found in $SRC_DIR or $SRC_DIR/olist"
    exit 1
  fi
  echo "Copied $count files to $DST_DIR"
  echo "Done. Verify files in $DST_DIR and update labs to use data/olist/*.csv"
else
  echo "Source directory $SRC_DIR not found."
  echo "Options:"
  echo "  1) Place the Olist CSV files manually under $DST_DIR"
  echo "  2) Download from Kaggle (requires kaggle CLI and credentials). See data/README.md for details."
  exit 2
fi
