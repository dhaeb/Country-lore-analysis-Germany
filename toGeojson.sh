#!/bin/bash

export FOLDER_NAME=$1
export TARGET_NAME=$2
export CSV_FILE="$TARGET_NAME.csv"

cd $FOLDER_NAME;
cat part* > $CSV_FILE
mv $CSV_FILE ..
cd ..
./non_cluster_processes/csv-to-geojson.groovy $CSV_FILE > "$TARGET_NAME.json"
