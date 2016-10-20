#!/bin/bash

R --no-save <<'EOF'
	source("analyse_scripts/init-sparkr.R")
  source("analyse_scripts/schemata.R")
  source("analyse_scripts/COR_FUNCTION.R")
  < analyse_scripts/siebenschlaefertag2z.R
  < analyse_scripts/siebenschlaefertag_COR.R
  < analyse_scripts/siebenschlaefertag_COUNT.R
  < analyse_scripts/tropfen_januar_schnee_mai.R
  < analyse_scripts/tropfen_januar_schnee_mai_COR.R
EOF
export RESULT_FOLDER="./Outputs"
for f in $(find $RESULT_FOLDER -name "*.csv"); do
  export FBASE=$(basename $f)
  export TARGET="$RESULT_FOLDER/${FBASE%.csv}_geo.json"
  echo $TARGET
  groovy non_cluster_processes/csv-to-geojson.groovy $f > $TARGET;
done