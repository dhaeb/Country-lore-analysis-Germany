## RUN januar_hell_sommer_heiss.R FIRST ##

# HELL UND WEISS
stimmtDf_SS <- where(df, df$meanSONNE > df$allMeanSONNE & df$meanSCHNEE > df$allMeanSCHNEE)

# stimmt in Deutschland? (overall)
a <- count(stimmtDf_SS)
# [1] 2082
b <- count(df)
# [1] 14996
stimmit_de_overall_SS <- a / b

# stimmt in Deutschland? (station)
cOverMeanDf_SS <- agg(group_by(stimmtDf_SS, "SID"), cOverMean = n(stimmtDf_SS$YEAR))
take(cOverMeanDf_SS,5L)
#    SID cOverMean
# 1 3231         4
# 2 3031         2
# 3 3631         9
# 4 2831         3
# 5  232        12

cOverMeanDf_SS <- withColumnRenamed(cOverMeanDf_SS, "SID", "ID")

# export stimmt for each station for visualisation
cFinal_SS <- join(cAllDf, cOverMeanDf_SS, cAllDf$SID == cOverMeanDf_SS$ID,"left_outer")
cFinal_SS$stimmt <- cFinal_SS$cOverMean / cFinal_SS$cAll
cFinal_SS <- select(cFinal_SS, cFinal_SS$SID, cFinal_SS$cAll, cFinal_SS$cOverMean, cFinal_SS$stimmt)
cFinal_SS <- join(cFinal_SS, stationDf, cFinal_SS$SID == stationDf$STATIONS_ID,"left_outer")
cFinal_SS <- arrange(cFinal_SS, desc(cFinal_SS$stimmt))
take(cFinal_SS,5L)
# SID cAll cOverMean    stimmt STATIONS_ID von_datum bis_datum Statationshoehe longitude latitude
# 1 4367    3         2 0.6666667        4367  19920401  20061231             175   52.0780   9.5520
# 2 7330    4         2 0.5000000        7330  20051001  20150421             159   51.4634   7.9780
# 3 3426    7         3 0.4285714        3426  19480401  20150422             125   51.5662  14.6989
# 4 1221   12         5 0.4166667        1221  19550101  19740228               0   53.3707   7.2236
# 5  738    8         3 0.3750000         738  19690501  19840831             310   50.3080   9.7835
# Stationsname          Bundesland Lage
# 1 Salzhemmendorf-Lauenstein       Niedersachsen    N
# 2           Arnsberg-Neheim Nordrhein-Westfalen    W
# 3               Muskau, Bad             Sachsen    O
# 4           Emden-Wolthusen       Niedersachsen    N
# 5     Br\xfcckenau, Bad (A)              Bayern    S

exportCsv_SS <- select(cFinal_SS, "cAll", "cOverMean", "stimmt", "longitude", "latitude", "Stationsname", "Bundesland", "Lage")

# stimmt es in N/S/W/O?: group_by Lage
stimmtLageDf_SS <- agg(group_by(exportCsv_SS,"Lage"), stimmtLage = sum(exportCsv_SS$cOverMean) / sum(exportCsv_SS$cAll))
take(stimmtLageDf_SS,4L)
# Lage stimmtLage
# 1    N  0.1248510
# 2    O  0.1325394
# 3    S  0.1520940
# 4    W  0.1363763

write.df(exportCsv_SS, "januar_hell_sommer_heiss_SS.csv", "com.databricks.spark.csv", "overwrite")

# chi-square tests
