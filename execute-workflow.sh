#!/bin/bash

R --no-save <<'EOF'
  source("analyse_scripts/init-sparkr.R")
  source("analyse_scripts/schemata.R")
  source("analyse_scripts/claf.R")
  analyseCountryLore(clInputJanHellSommerHeiss)
  analyseCountryLore(clInputNovFrostJanNass)
  analyseCountryLore(clInputOktoberWarmFein)
  analyseCountryLore(clInputOktoberNassKühl)
  analyseCountryLore(clInputMartiniWinter)
  analyseCountryLore(clInputFebruarNassJahrNass)
EOF
export RESULT_FOLDER="./intermeditate_results"
for f in $(find $RESULT_FOLDER -name "*.csv"); do
  export FBASE=$(basename $f)
  export TARGET="$RESULT_FOLDER/${FBASE%.csv}_geo.json"
  echo $TARGET
  groovy non_cluster_processes/csv-to-geojson.groovy $f > $TARGET;
done
