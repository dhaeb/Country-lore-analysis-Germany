# to prove "Ist der Januar hell und weiß, wird der Sommer sicher heiß."
# parameter1: sonne - SONNENSCHEINDAUER (Januar)
# parameter2: schnee - SCHNEEHOEHE (Januar)
# parameter3: luft - LUFTTEMPERATUR (Juli - August)

## load 'set_sqlContext.R' and 'schemata.R' first
source("/Users/martinmac/Country-lore-analysis-Germany/schemata.R")
source("/Users/martinmac/Country-lore-analysis-Germany/set_sqlContext.R")

# import data
inDf <- read.df(sqlContext,
                path = "/Users/martinmac/Big_Data_Prak_Lani/data/kl/kl_total.csv",
                source = "com.databricks.spark.csv",
                schema = klSchema,
                header="true", delimiter = ";")

# OUTPUT: S4 class that represents a DataFrame https://spark.apache.org/docs/1.5.0/api/R/DataFrame.html

# select needed columns
inDf <- select(inDf, "STATIONS_ID","MESS_DATUM","QUALITAETS_NIVEAU","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")
SparkR::nrow(inDf)
# [1] 12877275
SparkR::head(inDf)
# STATIONS_ID MESS_DATUM QUALITAETS_NIVEAU SONNENSCHEINDAUER SCHNEEHOEHE LUFTTEMPERATUR
# 1        6337 2004-08-01                10              12.2           0           18.8
# 2        6337 2004-08-02                10              11.7           0           20.0
# 3        6337 2004-08-03                10              12.2           0           21.3
# 4        6337 2004-08-04                10               6.8           0           21.4
# 5        6337 2004-08-05                10              10.5           0           24.1
# 6        6337 2004-08-06                10              11.8           0           24.6


### clean data ###
# take data with QUALITAETS_NIVEAU > 2 
inDf <- select(where(inDf, inDf$QUALITAETS_NIVEAU > 2),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")
# SparkR::nrow(inDf)
# [1] 12392463

# remove values = -999
# doesn't work
# inDf <- select(where(inDf, inDf$SONNENSCHEINDAUER != -999 | inDf$SCHNEEHOEHE != -999 | inDf$LUFTTEMPERATUR != -999),"STATIONS_ID","MESS_DATUM","QUALITAETS_NIVEAU","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")
inDf <- select(where(inDf, inDf$SONNENSCHEINDAUER != -999),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")
inDf <- select(where(inDf, inDf$SCHNEEHOEHE != -999),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")
inDf <- select(where(inDf, inDf$LUFTTEMPERATUR != -999 ), "STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")
SparkR::nrow(inDf)
# [1] 5507709

inDf$MONTH <- month(inDf$MESS_DATUM)
inDf$YEAR <- year(inDf$MESS_DATUM)
januar <- select(where(inDf, inDf$MONTH == 1),"STATIONS_ID","SONNENSCHEINDAUER","SCHNEEHOEHE","MONTH","YEAR")
sommer <- select(where(inDf, inDf$MONTH < 9 ), "STATIONS_ID", "LUFTTEMPERATUR","MONTH","YEAR")
sommer <- select(where(sommer, sommer$MONTH > 5 ), "STATIONS_ID", "LUFTTEMPERATUR","MONTH","YEAR")

# januar1 <- filter(januar, januar$SONNENSCHEINDAUER != -999) DOESN'T WORK

# calculate means of sonne, schnee, temp for each station and of ALL years
januarMeanDf <- agg(group_by(januar, "STATIONS_ID"), allMeanSONNE = mean(januar$SONNENSCHEINDAUER),allMeanSCHNEE = mean(januar$SCHNEEHOEHE))
sommerMeanDf <- agg(group_by(sommer, "STATIONS_ID"), allMeanTEMP = mean(sommer$LUFTTEMPERATUR))

# rename column
januarMeanDf <- withColumnRenamed(januarMeanDf, "STATIONS_ID", "id")
sommerMeanDf <- withColumnRenamed(sommerMeanDf,"STATIONS_ID","id")
SparkR::head(januarMeanDf)
# id allMeanSONNE allMeanSCHNEE
# 1 3631     1.598971     0.9814991
# 2 3031     1.848705     1.2324478
# 3 2831     1.659553     6.0570720
# 4 3231     1.490976     4.2150538
# 5 1032     1.343088     1.3963134
# 6 5832     1.695565     0.8064516

SparkR::head(sommerMeanDf)
# id allMeanTEMP
# 1 3631    16.23652
# 2 3031    17.44058
# 3 2831    16.97826
# 4 3231    16.62187
# 5 1032    16.50116
# 6 5832    16.71039

SparkR::nrow(januarMeanDf)
# [1] 583

SparkR::nrow(sommerMeanDf)
# [1] 589

# calculate means of sonne, schnee for each station and in EACH YEAR
januarMeanDf_y <- agg(group_by(januar, "STATIONS_ID","YEAR"), meanSONNE = mean(januar$SONNENSCHEINDAUER),meanSCHNEE = mean(januar$SCHNEEHOEHE))
SparkR::head(januarMeanDf_y)
# STATIONS_ID YEAR meanSONNE  meanSCHNEE
# 1        2783 1976 1.3612903  0.87096774
# 2        3631 2000 1.5967742  0.03225806
# 3        2444 1919 0.9193548  1.25806452
# 4        3621 1970 0.9096774 11.06451613
# 5         642 1993 1.7516129  0.00000000
# 6         150 1997 0.6290323  9.16129032

# join januar dfs
# januarDf <- select(januarDf, alias(januarDf$STATIONS_ID, "SID_JAN")) alias doesn't work for S4
januarDf <- join(januarMeanDf_y,januarMeanDf, januarMeanDf_y$STATIONS_ID == januarMeanDf$id)
januarDf <- select(januarDf, "STATIONS_ID","YEAR","meanSONNE","meanSCHNEE","allMeanSONNE","allMeanSCHNEE")
januarDf <- withColumnRenamed(januarDf, "STATIONS_ID", "SID_JAN")
januarDf <- withColumnRenamed(januarDf, "YEAR", "YEAR_JAN")
SparkR::nrow(januarDf)
# [1] 15308
SparkR::head(januarDf)
# SID_JAN YEAR_JAN meanSONNE meanSCHNEE allMeanSONNE allMeanSCHNEE
# 1    2831     1950 2.3000000  0.7419355     1.659553      6.057072
# 2    2831     1951 0.8935484  9.9354839     1.659553      6.057072
# 3    2831     1952 1.2516129  5.0967742     1.659553      6.057072
# 4    2831     1953 0.4838710  5.6774194     1.659553      6.057072
# 5    2831     1958 2.4258065  3.4516129     1.659553      6.057072
# 6    2831     1959 2.4387097  4.6129032     1.659553      6.057072

# calculate means of temp for each station and in each year
sommerMeanDf_y <- agg(group_by(sommer, "STATIONS_ID","YEAR"), meanTEMP = mean(sommer$LUFTTEMPERATUR))
sommerDf <- join(sommerMeanDf_y,sommerMeanDf, sommerMeanDf_y$STATIONS_ID == sommerMeanDf$id)
sommerDf <- select(sommerDf, "STATIONS_ID","YEAR","meanTEMP","allMeanTEMP")
sommerDf <- withColumnRenamed(sommerDf, "STATIONS_ID", "SID_SOM")
sommerDf <- withColumnRenamed(sommerDf, "YEAR", "YEAR_SOM")
SparkR::nrow(sommerDf)
# [1] 15035
SparkR::head(sommerDf)
# SID_SOM YEAR_SOM meanTEMP allMeanTEMP
# 1    2831     1949 16.06087    16.97826
# 2    2831     1950 18.37500    16.97826
# 3    2831     1951 16.70435    16.97826
# 4    2831     1952 18.23370    16.97826
# 5    2831     1958 16.71087    16.97826
# 6    2831     1959 17.32283    16.97826

# left join 2 dataframes by station AND year, left one is the smaller dataframe (januar)
df <- join(januarDf,sommerDf, januarDf$SID_JAN == sommerDf$SID_SOM & januarDf$YEAR_JAN == sommerDf$YEAR_SOM)
SparkR::head(df)
# SID_JAN YEAR_JAN meanSONNE meanSCHNEE allMeanSONNE allMeanSCHNEE SID_SOM YEAR_SOM meanTEMP allMeanTEMP
# 1     150     1997 0.6290323  9.1612903     1.362160      1.395161     150     1997 18.63043    17.91539
# 2     232     1963 2.5161290 18.6774194     1.950775      3.834915     232     1963 16.96957    17.03808
# 3     259     1964 1.1096774  0.6129032     1.227680      2.697190     259     1964 18.73696    17.74899
# 4     480     1987 2.1258065  5.3548387     1.244841      4.671975     480     1987 14.85978    15.76203
# 5     502     1973 0.8000000  0.4838710     1.171464      7.551696     502     1973 16.42391    16.16552
# 6     555     2012 1.6181935  0.0000000     1.579423      2.423828     555     2012 17.14239    17.68458
SparkR::nrow(df)
# [1] 14690

df <- withColumnRenamed(df, "SID_JAN", "SID")
df <- withColumnRenamed(df, "YEAR_JAN", "YEAR")

# stimmt es in Deutschland?
# OUTPUT: # all three parameters are true and # of all cases ######  df$meanSCHNEE > 0
stimmtDf <- where(df, df$meanSONNE > df$allMeanSONNE & df$meanSCHNEE > 0 & df$meanTEMP > df$allMeanTEMP)
SparkR::head(stimmtDf)
# SID YEAR meanSONNE meanSCHNEE allMeanSONNE allMeanSCHNEE SID_SOM YEAR_SOM meanTEMP allMeanTEMP
# 1 1032 1963  1.638710   5.935484     1.343088     1.3963134    1032     1963 17.11250    16.50116
# 2 2074 2009  3.558065   1.677419     2.257631     1.6415771    2074     2009 17.16196    17.15498
# 3 3096 1995  1.632258   9.322581     1.480108     7.9976105    3096     1995 17.10217    15.29413
# 4 3485 2002  1.812903   2.354839     1.647926     2.2741935    3485     2002 17.72826    17.16801
# 5 3730 1937  3.051613  31.774194     2.498264    30.4156799    3730     1937 14.86630    14.74469
# 6  766 2006  2.248276   1.000000     1.803297     0.8681319     766     2006 17.12111    16.59444
# nrow(januarMeanDf_y)
# [1] 15368
# nrow(sommerMeanDf_y)
# [1] 15368

# stimmt in Deutschland? (overall)
a <- count(stimmtDf)
a
# [1] 909
b <- count(df)
b
# [1] 14690
stimmit_de_overall <- a / b
stimmit_de_overall
# [1] 0.06187883

# stimmt in Deutschland? (station)
cAllDf <- agg(group_by(df, "SID"), cAll = n(df$YEAR))
SparkR::head(cAllDf)
# SID cAll
# 1 3031   33
# 2 3231   24
# 3 3631   68
# 4 2831   12
# 5  232   68
# 6 1032   14

cOverMeanDf <- agg(group_by(stimmtDf, "SID"), cOverMean = n(stimmtDf$YEAR))
SparkR::head(cOverMeanDf)
# SID cOverMean
# 1 3231         2
# 2 3031         2
# 3 3631         5
# 4 2831         1
# 5 1032         1
# 6 3032         2

cOverMeanDf <- withColumnRenamed(cOverMeanDf, "SID", "ID")

## read station data
stationDf <- read.df(sqlContext,
                     "/Users/martinmac/Big_Data_Prak_Lani/data/kl/KL_Tageswerte_Beschreibung_Stationen4.txt",
                     "com.databricks.spark.csv",
                     metaSchema,
                     header="true", delimiter = "\t")
SparkR::nrow(stationDf)
# [1] 1054
# [1] 585 (old output, big diff!)

# export stimmt for each station for visualisation
cFinal <- join(cAllDf, cOverMeanDf, cAllDf$SID == cOverMeanDf$ID)
cFinal$stimmt <- cFinal$cOverMean / cFinal$cAll
cFinal <- select(cFinal, cFinal$SID, cFinal$cAll, cFinal$cOverMean, cFinal$stimmt)
cFinal <- join(cFinal, stationDf, cFinal$SID == stationDf$STATIONS_ID)
cFinal <- arrange(cFinal, desc(cFinal$stimmt))
SparkR::head(cFinal)
# SID cAll cOverMean    stimmt STATIONS_ID von_datum bis_datum Statationshoehe longitude latitude
# 1 6170    3         1 0.3333333        6170  20000201  20150422              40   52.0192  14.7254
# 2  766    3         1 0.3333333         766  20000701  20150422             477   50.1746   7.0595
# 3 4367    3         1 0.3333333        4367  19920401  20061231             175   52.0780   9.5520
# 4 4847    3         1 0.3333333        4847  19700101  19751231             500   51.3629   9.6902
# 5  474    3         1 0.3333333         474  19590101  19860930             599   48.1252   9.7639
# 6 2796    3         1 0.3333333        2796  20000601  20150422              40   53.9156  12.2790
# Stationsname             Bundesland Lage
# 1                   Coschen            Brandenburg    O
# 2     B\xfcchel (Flugplatz)        Rheinland-Pfalz    W
# 3 Salzhemmendorf-Lauenstein          Niedersachsen    N
# 4                 Steinberg          Niedersachsen    N
# 5     Warthausen-Birkenhard   Baden-W\xfcrttemberg    S
# 6         Laage (Flugplatz) Mecklenburg-Vorpommern    O

SparkR::nrow(cFinal)
# [1] 384

exportCsv <- select(cFinal, "cAll", "cOverMean", "stimmt", "longitude", "latitude", "Stationsname", "Bundesland", "Lage","Statationshoehe","von_datum","bis_datum")

# stimmt es in N/S/W/O?: group_by Lage
stimmtLageDf <- agg(group_by(exportCsv,"Lage"), stimmtLage = sum(exportCsv$cOverMean) / sum(exportCsv$cAll))
SparkR::head(stimmtLageDf)
# Lage stimmtLage
# 1    N 0.06384409
# 2    O 0.08922477
# 3    S 0.07167309
# 4    W 0.06296992

write.df(exportCsv, "/Users/martinmac/Big_Data_Prak_Lani/januar_hell_sommer_heiss.csv", "com.databricks.spark.csv", "overwrite")
# OUTPUT: /Users/martinmac/Big_Data_Prak_Lani/januar_hell_sommer_heiss.csv

# Rdf <- collect(sparkDF)
# chi-square tests


