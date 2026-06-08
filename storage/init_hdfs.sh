#!/usr/bin/env bash
# Run once after Hadoop is up to create the HDFS directory structure.
set -e

HDFS="hdfs dfs"

echo "Creating HDFS zone directories..."

$HDFS -mkdir -p /data/raw/egx/ohlcv
$HDFS -mkdir -p /data/raw/egx/ticks
$HDFS -mkdir -p /data/staging/egx/ohlcv
$HDFS -mkdir -p /data/curated/egx/ohlcv
$HDFS -mkdir -p /data/curated/egx/indicators

echo "Setting permissions..."
$HDFS -chmod -R 775 /data

echo "HDFS structure ready:"
$HDFS -ls -R /data
