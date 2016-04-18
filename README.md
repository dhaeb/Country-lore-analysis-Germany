# Country-lore-analysis-Germany
Project using sparkR to check if german country lores regarding weather data are true.

#### workflow
raw data --> data with specific schema --> SparkR analysis --> R-outputs (.csv) --> Geojson --> visualization

#### country lores and analysis
The country lores are classified as two types: true/false rules and correlation rules. Different methods are used to analyse different types of rules.

###### true/false rules
The following rules are classified as true/false rules:
1. Ist der Januar hell und weiß, wird der Sommer sicher heiß.
2. Gefriert im November schon das Wasser, wird der Januar umso nasser.
3. Ist der Oktober warm und fein, kommt ein scharfer Winter drein. Ist er aber nass und kühl, mild der Winter werden will.
4. Hat Martini einen weißen Bart, dann wird der Winter lang und hart.
5. Je nasser ist der Februar, desto nasser wird das ganze Jahr. 

**These rules are analyzed as the workflow states in: execute-workflow.sh**

###### coorelation rules
The following rules are classified as correlation rules:
1. So viele Tropfen im Januar, so viel Schnee im Mai
2. Wie's Wetter am Siebenschläfertag, so bleibt es sieben Wochen danach

**These rules are analyzed as the workflow states in: execute-workflow_cor.sh**

Coorelation analyses also produce outputs in COR_latex and Plots.

