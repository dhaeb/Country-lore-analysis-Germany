# Country-lore-analysis-Germany
Project using sparkR to check if german country lores regarding weather data are true.

## workflow
raw data --> data with specific schema --> SparkR analysis --> R-outputs (.csv) --> Geojson --> visualization

## country lores and analysis
The country lores are classified as two types: true/false rules and correlation rules. Different methods are used to analyse different types of rules.

### true/false rules
The following rules are analyzed as true/false rules:
- Ist der Januar hell und weiß, wird der Sommer sicher heiß.
- Gefriert im November schon das Wasser, wird der Januar umso nasser.
- Ist der Oktober warm und fein, kommt ein scharfer Winter drein. Ist er aber nass und kühl, mild der Winter werden will.
- Hat Martini einen weißen Bart, dann wird der Winter lang und hart.
- Je nasser ist der Februar, desto nasser wird das ganze Jahr. 
TODO: I weiss es nicht mehr, wie diese Regel lautet: analyseCountryLore(clInputOktoberNassKühl)

* These rules are analyzed as the workflow states in: execute-workflow.sh

### coorelation rules
The following rules are analyzed as correlation rules:
- So viele Tropfen im Januar, so viel Schnee im Mai
- Wie's Wetter am Siebenschläfertag, so bleibt es sieben Wochen danach

* These rules are analyzed as the workflow states in: execute-workflow_cor.sh

Coorelation analyses also produce outputs in COR_latex and Plots.


