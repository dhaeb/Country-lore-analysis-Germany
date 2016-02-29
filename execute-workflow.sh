#!/bin/bash

R --no-save <<'EOF'
source("analyse_scripts/init-sparkr.R")
source("analyse_scripts/schemata.R")
source("analyse_scripts/claf.R")
inputScenarios <- c(clInputJanHellSommerHeiss, clInputNovFrostJanNass, clInputOktoberWarmFein, clInputOktoberNassKühl, clInputFebruarNassJahrNass, clInputMartiniWinter)
for(scenario in inputScenarios){
  analyseCountryLore(scenario)
}
EOF
export RESULT_FOLDER="./intermeditate_results"
for f in $(find $RESULT_FOLDER -name "*.csv"); do
  export TARGET="${f%.csv}_geo.json"
  echo $TARGET
  groovy non_cluster_processes/csv-to-geojson.groovy $f > $TARGET;
done