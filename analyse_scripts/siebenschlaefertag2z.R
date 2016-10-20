# Wie's Wetter am Siebenschläfertag, so bleibt es sieben Wochen danach.
# parameter1: sonne - SONNENSCHEINDAUER (26.06 - 28.06 vs 29.06 - 17.08)
# parameter2: regen - NIEDERSCHLAGSHOEHE (26.06 - 28.06 vs 29.06 - 17.08)
# parameter3: temp - LUFTTEMPERATUR (26.06 - 28.06 vs 29.06 - 17.08)

# a case:= specific station in specific year
setwd("/Users/martinmac/Country-lore-analysis-Germany/")

klSchema <- structType(
    structField("STATIONS_ID", "integer"),
    structField("MESS_DATUM", "date"),
    structField("QUALITAETS_NIVEAU", "double"),
    structField("LUFTTEMPERATUR", "double"),
    structField("DAMPFDRUCK", "double"),
    structField("BEDECKUNGSGRAD", "double"),
    structField("LUFTDRUCK_STATIONSHOEHE", "double"),
    structField("REL_FEUCHTE", "double"),
    structField("WINDGESCHWINDIGKEIT", "double"),
    structField("LUFTTEMPERATUR_MAXIMUM", "double"),
    structField("LUFTTEMPERATUR_MINIMUM", "double"),
    structField("LUFTTEMP_AM_ERDB_MINIMUM", "double"),
    structField("WINDSPITZE_MAXIMUM", "double"),
    structField("NIEDERSCHLAGSHOEHE", "double"),
    structField("NIEDERSCHLAGSHOEHE_IND", "double"),
    structField("SONNENSCHEINDAUER", "double"),
    structField("SCHNEEHOEHE", "double")
)

# import data
inDf1 <- read.df(sqlContext,
                 path = "data/kl_total.csv",
                 source = "com.databricks.spark.csv",
                 schema = klSchema,
                 header="true", delimiter = ";")
# select needed columns
inDf1 <- SparkR::select(inDf1, "STATIONS_ID","MESS_DATUM","QUALITAETS_NIVEAU","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
# nrow(inDf1)
# [1] 12877275

# inDf554 <- SparkR::select(where(inDf1, inDf1$STATIONS_ID == 554), "STATIONS_ID","MESS_DATUM","QUALITAETS_NIVEAU","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
# SparkR::head(inDf554)
# write.df(inDf554, "Outputs/s554.csv", "com.databricks.spark.csv", "overwrite")

# SparkR::head(inDf1)
# STATIONS_ID MESS_DATUM QUALITAETS_NIVEAU SONNENSCHEINDAUER NIEDERSCHLAGSHOEHE LUFTTEMPERATUR
# 1        6337 2004-08-01                10              12.2                  0           18.8
# 2        6337 2004-08-02                10              11.7                  0           20.0
# 3        6337 2004-08-03                10              12.2                  0           21.3
# 4        6337 2004-08-04                10               6.8                  0           21.4
# 5        6337 2004-08-05                10              10.5                  0           24.1
# 6        6337 2004-08-06                10              11.8                  0           24.6

### clean data ###
# QUALITAETS_NIVEAU > 2
inDf1 <- select(where(inDf1, inDf1$QUALITAETS_NIVEAU > 2),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
# SparkR::nrow(inDf1)
# [1] 12392463

# remove values = -999
inDf1 <- select(where(inDf1, inDf1$SONNENSCHEINDAUER != -999),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
inDf1 <- select(where(inDf1, inDf1$NIEDERSCHLAGSHOEHE != -999),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
inDf1 <- select(where(inDf1, inDf1$LUFTTEMPERATUR != -999 ), "STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")

# select needed rows
# inDf1$DATEYEAR <- dayofyear(inDf1$MESS_DATUM)
inDf1$DATE <- dayofmonth(inDf1$MESS_DATUM)
inDf1$MONTH <- month(inDf1$MESS_DATUM)
inDf1$YEAR <- year(inDf1$MESS_DATUM)
# SparkR::head(inDf1)
# STATIONS_ID MESS_DATUM SONNENSCHEINDAUER NIEDERSCHLAGSHOEHE LUFTTEMPERATUR DATE MONTH YEAR
# 1        6337 2004-08-01              12.2                  0           18.8    1     8 2004
# 2        6337 2004-08-02              11.7                  0           20.0    2     8 2004
# 3        6337 2004-08-03              12.2                  0           21.3    3     8 2004
# 4        6337 2004-08-04               6.8                  0           21.4    4     8 2004
# 5        6337 2004-08-05              10.5                  0           24.1    5     8 2004
# 6        6337 2004-08-06              11.8                  0           24.6    6     8 2004
# SparkR::nrow(inDf1)
# [1] 6093611

juniDf <- select(where(inDf1, inDf1$MONTH == 6),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR","DATE","MONTH","YEAR")
julyDf <- select(where(inDf1, inDf1$MONTH == 7),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR","DATE","MONTH","YEAR")
augustDf <- select(where(inDf1, inDf1$MONTH == 8),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR","DATE","MONTH","YEAR")
# SparkR::head(juniDf)
# STATIONS_ID MESS_DATUM SONNENSCHEINDAUER NIEDERSCHLAGSHOEHE LUFTTEMPERATUR DATE MONTH YEAR
# 1        6337 2005-06-01              11.1                0.3           11.3    1     6 2005
# 2        6337 2005-06-02               1.0                0.0           15.3    2     6 2005
# 3        6337 2005-06-03              10.3                0.2           19.2    3     6 2005
# 4        6337 2005-06-04               3.2                6.1           14.0    4     6 2005
# 5        6337 2005-06-05               1.1               13.5           13.6    5     6 2005
# 6        6337 2005-06-06               1.5                0.0           11.5    6     6 2005

tageDf <- select(where(juniDf, juniDf$DATE > 25 & juniDf$DATE < 29 ),"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
juniWDf <- select(where(juniDf, juniDf$DATE > 28),"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
julyWDf <- select(julyDf,"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
augustWDf <- select(where(augustDf, augustDf$DATE < 18),"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
wochenDf <- rbind(juniWDf,julyWDf,augustWDf)
# extra Zeitraum: 25.Juni - 7.Juli
tage2Juni <- select(where(juniDf, juniDf$DATE > 24 ),"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
tage2Juli <- select(where(julyDf, julyDf$DATE < 8 ),"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
tage2Df <- rbind(tage2Juni,tage2Juli)
july2WDf <- select(where(julyDf, julyDf$DATE > 7),"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
wochen2Df <- rbind(july2WDf,augustWDf)
# nrow(tageDf)
# [1] 50025

# nrow(wochenDf)
# [1] 834893

# nrow(tage2Df)
# [1] 216959

# SparkR::head(tageDf)
# STATIONS_ID YEAR SONNENSCHEINDAUER NIEDERSCHLAGSHOEHE LUFTTEMPERATUR
# 1        6337 2005              10.5                0.0           17.0
# 2        6337 2005              14.7                0.0           18.5
# 3        6337 2005              15.1                2.3           18.6
# 4        6337 2006               5.5                0.0           18.9
# 5        6337 2006               0.5                0.0           15.6
# 6        6337 2006               4.3                0.0           16.1

# SparkR::head(wochenDf)
# STATIONS_ID YEAR SONNENSCHEINDAUER NIEDERSCHLAGSHOEHE LUFTTEMPERATUR
# 1        6337 2005               4.2                1.5           17.9
# 2        6337 2005               1.6               10.0           17.4
# 3        6337 2006              14.0                0.0           18.0
# 4        6337 2006              14.8                0.0           20.2
# 5        6337 2007               0.8               13.7           14.5
# 6        6337 2007               1.3                0.0           16.1

# take mean of each parameter for each station and each year
tageMeanDf <- agg(group_by(tageDf,"STATIONS_ID","YEAR"), allMeanSONNE = mean(tageDf$SONNENSCHEINDAUER), allMeanREGEN = mean(tageDf$NIEDERSCHLAGSHOEHE), allMeanTEMP = mean(tageDf$LUFTTEMPERATUR))
wochenMeanDf <- agg(group_by(wochenDf,"STATIONS_ID","YEAR"), allMeanSONNEw = mean(wochenDf$SONNENSCHEINDAUER), allMeanREGENw = mean(wochenDf$NIEDERSCHLAGSHOEHE), allMeanTEMPw = mean(wochenDf$LUFTTEMPERATUR))
tage2MeanDf <- agg(group_by(tage2Df,"STATIONS_ID","YEAR"), allMeanSONNE2 = mean(tage2Df$SONNENSCHEINDAUER), allMeanREGEN2 = mean(tage2Df$NIEDERSCHLAGSHOEHE), allMeanTEMP2 = mean(tage2Df$LUFTTEMPERATUR))
wochen2MeanDf <- agg(group_by(wochen2Df,"STATIONS_ID","YEAR"), allMeanSONNEw2 = mean(wochen2Df$SONNENSCHEINDAUER), allMeanREGENw2 = mean(wochen2Df$NIEDERSCHLAGSHOEHE), allMeanTEMPw2 = mean(wochen2Df$LUFTTEMPERATUR))

tageMeanDf <- withColumnRenamed(tageMeanDf, "STATIONS_ID", "SID")
tageMeanDf <- withColumnRenamed(tageMeanDf, "YEAR", "YEAR")
wochenMeanDf <- withColumnRenamed(wochenMeanDf, "STATIONS_ID", "SID_W")
wochenMeanDf <- withColumnRenamed(wochenMeanDf, "YEAR", "YEAR_W")
tage2MeanDf <- withColumnRenamed(tage2MeanDf, "STATIONS_ID", "SID_2")
tage2MeanDf <- withColumnRenamed(tage2MeanDf, "YEAR", "YEAR_2")
wochen2MeanDf <- withColumnRenamed(wochen2MeanDf, "STATIONS_ID", "SID_W2")
wochen2MeanDf <- withColumnRenamed(wochen2MeanDf, "YEAR", "YEAR_W2")

# join tage and wochen meanDfs
joinedDf <- join(tageMeanDf, wochenMeanDf, tageMeanDf$SID == wochenMeanDf$SID_W & tageMeanDf$YEAR == wochenMeanDf$YEAR_W)
# nrow(joinedDf)
# [1] 16694

# join second period of siebenschlaefertage
joinedDft <- join(joinedDf, tage2MeanDf, joinedDf$SID == tage2MeanDf$SID_2 & joinedDf$YEAR == tage2MeanDf$YEAR_2)
# nrow(joinedDf2)
# [1] 16694

joinedDf2 <- join(joinedDft, wochen2MeanDf, joinedDft$SID == wochen2MeanDf$SID_W2 & joinedDft$YEAR == wochen2MeanDf$YEAR_W2)

# de-select SID_W, YEAR_W, SID_2, YEAR_2###
joinedDf_final <- select(joinedDf2,"SID", "YEAR", "allMeanSONNE", "allMeanREGEN", "allMeanTEMP","allMeanSONNE2", "allMeanREGEN2", "allMeanTEMP2", "allMeanSONNEw", "allMeanREGENw","allMeanTEMPw","allMeanSONNEw2", "allMeanREGENw2","allMeanTEMPw2" )

# save joinedDf for plotting
write.df(joinedDf_final, "siebenschlaefertag2z", "com.databricks.spark.csv", "overwrite")

output <- SparkR:::collect(joinedDf_final)
outfile = "Outputs/intermediateCSV/sieben2z.csv"
write.csv(output,outfile,row.names=FALSE)
