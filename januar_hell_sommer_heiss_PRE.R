# import data
inDf <- read.df(sqlContext,
                path = "/Users/martinmac/Big_Data_Prak_Lani/data/kl/kl_total.csv",
                source = "com.databricks.spark.csv",
                schema = klSchema,
                header="true", delimiter = ";")

# select needed columns
inDf <- select(inDf, "STATIONS_ID","MESS_DATUM","QUALITAETS_NIVEAU","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")

inDf <- select(where(inDf, inDf$SONNENSCHEINDAUER != -999),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")
inDf <- select(where(inDf, inDf$SCHNEEHOEHE != -999),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")
inDf <- select(where(inDf, inDf$LUFTTEMPERATUR != -999 ), "STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")

inDf$MONTH <- month(inDf$MESS_DATUM)
inDf$YEAR <- year(inDf$MESS_DATUM)
januar <- select(where(inDf, inDf$MONTH == 1),"STATIONS_ID","SONNENSCHEINDAUER","SCHNEEHOEHE","MONTH","YEAR")
sommer <- select(where(inDf, inDf$MONTH < 9 ), "STATIONS_ID", "LUFTTEMPERATUR","MONTH","YEAR")
sommer <- select(where(sommer, sommer$MONTH > 5 ), "STATIONS_ID", "LUFTTEMPERATUR","MONTH","YEAR")

# calculate means of sonne, schnee, temp for each station and of ALL years
januarMeanDf <- agg(group_by(januar, "STATIONS_ID"), allMeanSONNE = mean(januar$SONNENSCHEINDAUER),allMeanSCHNEE = mean(januar$SCHNEEHOEHE))
sommerMeanDf <- agg(group_by(sommer, "STATIONS_ID"), allMeanTEMP = mean(sommer$LUFTTEMPERATUR))

# rename column
januarMeanDf <- withColumnRenamed(januarMeanDf, "STATIONS_ID", "id")
sommerMeanDf <- withColumnRenamed(sommerMeanDf,"STATIONS_ID","id")

# calculate means of sonne, schnee for each station and in EACH YEAR
januarMeanDf_y <- agg(group_by(januar, "STATIONS_ID","YEAR"), meanSONNE = mean(januar$SONNENSCHEINDAUER),meanSCHNEE = mean(januar$SCHNEEHOEHE))

# join januar dfs
# januarDf <- select(januarDf, alias(januarDf$STATIONS_ID, "SID_JAN")) alias doesn't work for S4
januarDf <- join(januarMeanDf_y,januarMeanDf, januarMeanDf_y$STATIONS_ID == januarMeanDf$id,"left_outer")
januarDf <- select(januarDf, "STATIONS_ID","YEAR","meanSONNE","meanSCHNEE","allMeanSONNE","allMeanSCHNEE")
januarDf <- withColumnRenamed(januarDf, "STATIONS_ID", "SID_JAN")
januarDf <- withColumnRenamed(januarDf, "YEAR", "YEAR_JAN")

# calculate means of temp for each station and in each year
sommerMeanDf_y <- agg(group_by(sommer, "STATIONS_ID","YEAR"), meanTEMP = mean(sommer$LUFTTEMPERATUR))
sommerDf <- join(sommerMeanDf_y,sommerMeanDf, sommerMeanDf_y$STATIONS_ID == sommerMeanDf$id,"left_outer")
sommerDf <- select(sommerDf, "STATIONS_ID","YEAR","meanTEMP","allMeanTEMP")
sommerDf <- withColumnRenamed(sommerDf, "STATIONS_ID", "SID_SOM")
sommerDf <- withColumnRenamed(sommerDf, "YEAR", "YEAR_SOM")

# left join 2 dataframes by station AND year, left one is the smaller dataframe (januar)
leftDf <- join(januarDf,sommerDf, januarDf$SID_JAN == sommerDf$SID_SOM & januarDf$YEAR_JAN == sommerDf$YEAR_SOM ,"left_outer")
leftDf <- withColumnRenamed(leftDf, "SID_JAN", "SID")
leftDf <- withColumnRenamed(leftDf, "YEAR_JAN", "YEAR")

# check if after join, large df's value != NA
## Please keep in mind that there is no distinction between NA and NaN in SparkR.
df <- dropna(leftDf)
cAllDf <- agg(group_by(df, "SID"), cAll = n(df$YEAR))
cache(cAllDf)

## read station data
stationDf <- read.df(sqlContext,
                     "/Users/martinmac/Big_Data_Prak_Lani/data/kl/KL_Tageswerte_Beschreibung_Stationen4.txt",
                     "com.databricks.spark.csv",
                     metaSchema,
                     header="true", delimiter = "\t")

###### SONNE ########
stimmtDf_SON <- where(df, df$meanSONNE > df$allMeanSONNE)

# stimmt in Deutschland? (overall)
a_SON <- count(stimmtDf_SON)

# stimmt in Deutschland? (station)
cOverMeanDf_SON<- agg(group_by(stimmtDf_SON, "SID"), cOverMean = n(stimmtDf_SON$YEAR))
cOverMeanDf_SON<- withColumnRenamed(cOverMeanDf_SON, "SID", "ID")

# export stimmt for each station for visualisation
cFinal_SON <- join(cAllDf, cOverMeanDf_SON, cAllDf$SID == cOverMeanDf_SON$ID,"left_outer")
cFinal_SON$stimmt <- cFinal_SON$cOverMean / cFinal_SON$cAll
cFinal_SON <- select(cFinal_SON, cFinal_SON$SID, cFinal_SON$cAll, cFinal_SON$cOverMean, cFinal_SON$stimmt)
cFinal_SON <- join(cFinal_SON, stationDf, cFinal_SON$SID == stationDf$STATIONS_ID,"left_outer")
cFinal_SON <- arrange(cFinal_SON, desc(cFinal_SON$stimmt))

exportCsv_SON <- select(cFinal_SON, "cAll", "cOverMean", "stimmt", "longitude", "latitude", "Stationsname", "Bundesland", "Lage")

# stimmt es in N/S/W/O?: group_by Lage
stimmtLageDf_SON <- agg(group_by(exportCsv_SON,"Lage"), stimmtLage = sum(exportCsv_SON$cOverMean) / sum(exportCsv_SON$cAll))
take(stimmtLageDf_SON,4L)
# Lage stimmtLage
# 1    N  0.4567938
# 2    O  0.4763734
# 3    S  0.4649155
# 4    W  0.4634011

write.df(exportCsv_SON, "januar_hell_sommer_heiss_SON.csv", "com.databricks.spark.csv", "overwrite")

##### SCHNEE ##########
stimmtDf_SCH <- where(df, df$meanSCHNEE > df$allMeanSCHNEE)

# stimmt in Deutschland? (overall)
a_SCH <- count(stimmtDf_SCH)
# a_SCH
# [1] 4868

# stimmt in Deutschland? (station)
cOverMeanDf_SCH<- agg(group_by(stimmtDf_SCH, "SID"), cOverMean = n(stimmtDf_SCH$YEAR))
cOverMeanDf_SCH<- withColumnRenamed(cOverMeanDf_SCH, "SID", "ID")

# export stimmt for each station for visualisation
cFinal_SCH <- join(cAllDf, cOverMeanDf_SCH, cAllDf$SID == cOverMeanDf_SCH$ID,"left_outer")
cFinal_SCH$stimmt <- cFinal_SCH$cOverMean / cFinal_SCH$cAll
cFinal_SCH <- select(cFinal_SCH, cFinal_SCH$SID, cFinal_SCH$cAll, cFinal_SCH$cOverMean, cFinal_SCH$stimmt)
cFinal_SCH <- join(cFinal_SCH, stationDf, cFinal_SCH$SID == stationDf$STATIONS_ID,"left_outer")
cFinal_SCH <- arrange(cFinal_SCH, desc(cFinal_SCH$stimmt))

exportCsv_SCH <- select(cFinal_SCH, "cAll", "cOverMean", "stimmt", "longitude", "latitude", "Stationsname", "Bundesland", "Lage")

# stimmt es in N/S/W/O?: group_by Lage
stimmtLageDf_SCH <- agg(group_by(exportCsv_SCH,"Lage"), stimmtLage = sum(exportCsv_SCH$cOverMean) / sum(exportCsv_SCH$cAll))
take(stimmtLageDf_SCH,4L)
# Lage stimmtLage
# 1    N  0.2643027
# 2    O  0.3203995
# 3    S  0.3835415
# 4    W  0.2947398

write.df(exportCsv_SCH, "januar_hell_sommer_heiss_SCH.csv", "com.databricks.spark.csv", "overwrite")

###### TEMP ###########
stimmtDf_TEMP <- where(df, df$meanTEMP > df$allMeanTEMP)

# stimmt in DeutTEMPland? (overall)
a_TEMP <- count(stimmtDf_TEMP)
# a_TEMP
# [1] 7023

# stimmt in DeutTEMPland? (station)
cOverMeanDf_TEMP<- agg(group_by(stimmtDf_TEMP, "SID"), cOverMean = n(stimmtDf_TEMP$YEAR))

cOverMeanDf_TEMP<- withColumnRenamed(cOverMeanDf_TEMP, "SID", "ID")

# export stimmt for each station for visualisation
cFinal_TEMP <- join(cAllDf, cOverMeanDf_TEMP, cAllDf$SID == cOverMeanDf_TEMP$ID,"left_outer")
cFinal_TEMP$stimmt <- cFinal_TEMP$cOverMean / cFinal_TEMP$cAll
cFinal_TEMP <- select(cFinal_TEMP, cFinal_TEMP$SID, cFinal_TEMP$cAll, cFinal_TEMP$cOverMean, cFinal_TEMP$stimmt)
cFinal_TEMP <- join(cFinal_TEMP, stationDf, cFinal_TEMP$SID == stationDf$STATIONS_ID,"left_outer")
cFinal_TEMP <- arrange(cFinal_TEMP, desc(cFinal_TEMP$stimmt))

exportCsv_TEMP <- select(cFinal_TEMP, "cAll", "cOverMean", "stimmt", "longitude", "latitude", "Stationsname", "Bundesland", "Lage")

# stimmt es in N/S/W/O?: group_by Lage
stimmtLageDf_TEMP <- agg(group_by(exportCsv_TEMP,"Lage"), stimmtLage = sum(exportCsv_TEMP$cOverMean) / sum(exportCsv_TEMP$cAll))
take(stimmtLageDf_TEMP,4L)
#  Lage stimmtLage
# 1    N  0.4705006
# 2    O  0.4882828
# 3    S  0.4597722
# 4    W  0.4647927

write.df(exportCsv_TEMP, "januar_hell_sommer_heiss_TEMP.csv", "com.databricks.spark.csv", "overwrite")
