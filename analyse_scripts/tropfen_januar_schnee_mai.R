# So viele Tropfen im Januar, so viel Schnee im Mai
# parameter2: regen - NIEDERSCHLAGSHOEHE (Januar)
# parameter3: schnee - SCHNEEHOEHE (Mai)
setwd("/Users/martinmac/Country-lore-analysis-Germany/")

inDf2 <- read.df(sqlContext,
                 path = "data/kl_total.csv",
                 source = "com.databricks.spark.csv",
                 schema = klSchema,
                 header="true", delimiter = ";")
# select needed columns
inDf2 <- SparkR::select(inDf2, "STATIONS_ID","MESS_DATUM","QUALITAETS_NIVEAU","NIEDERSCHLAGSHOEHE","SCHNEEHOEHE")
# nrow(inDf2)
# [1] 12877275
# SparkR::head(inDf2)
# STATIONS_ID MESS_DATUM QUALITAETS_NIVEAU NIEDERSCHLAGSHOEHE SCHNEEHOEHE
# 1        6337 2004-08-01                10                  0           0
# 2        6337 2004-08-02                10                  0           0
# 3        6337 2004-08-03                10                  0           0
# 4        6337 2004-08-04                10                  0           0
# 5        6337 2004-08-05                10                  0           0
# 6        6337 2004-08-06                10                  0           0

### clean data ###
# QUALITAETS_NIVEAU > 2
inDf2 <-  SparkR::select(where(inDf2, inDf2$QUALITAETS_NIVEAU > 2),"STATIONS_ID","MESS_DATUM","NIEDERSCHLAGSHOEHE","SCHNEEHOEHE")
# SparkR::nrow(inDf2)
# [1] 12392463
# STATIONS_ID MESS_DATUM QUALITAETS_NIVEAU NIEDERSCHLAGSHOEHE SCHNEEHOEHE
# 1        6337 2004-08-01                10                  0           0
# 2        6337 2004-08-02                10                  0           0
# 3        6337 2004-08-03                10                  0           0
# 4        6337 2004-08-04                10                  0           0
# 5        6337 2004-08-05                10                  0           0
# 6        6337 2004-08-06                10                  0           0

# remove values = -999
inDf2 <-  SparkR::select(where(inDf2, inDf2$NIEDERSCHLAGSHOEHE != -999),"STATIONS_ID","MESS_DATUM","NIEDERSCHLAGSHOEHE","SCHNEEHOEHE")
inDf2 <-  SparkR::select(where(inDf2, inDf2$SCHNEEHOEHE != -999),"STATIONS_ID","MESS_DATUM","NIEDERSCHLAGSHOEHE","SCHNEEHOEHE")

# select needed rows
inDf2$DATE <- dayofmonth(inDf2$MESS_DATUM)
inDf2$MONTH <- month(inDf2$MESS_DATUM)
inDf2$YEAR <- year(inDf2$MESS_DATUM)
# SparkR::head(inDf2)
# STATIONS_ID MESS_DATUM NIEDERSCHLAGSHOEHE SCHNEEHOEHE DATE MONTH YEAR
# 1        6337 2004-08-01                  0           0    1     8 2004
# 2        6337 2004-08-02                  0           0    2     8 2004
# 3        6337 2004-08-03                  0           0    3     8 2004
# 4        6337 2004-08-04                  0           0    4     8 2004
# 5        6337 2004-08-05                  0           0    5     8 2004
# 6        6337 2004-08-06                  0           0    6     8 2004

janDf <-  SparkR::select(where(inDf2, inDf2$MONTH == 1),"STATIONS_ID","MESS_DATUM","NIEDERSCHLAGSHOEHE","SCHNEEHOEHE","DATE","MONTH","YEAR")
maiDf <-  SparkR::select(where(inDf2, inDf2$MONTH == 5),"STATIONS_ID","MESS_DATUM","NIEDERSCHLAGSHOEHE","SCHNEEHOEHE","DATE","MONTH","YEAR")
# nrow(janDf)
# [1] 949709
# nrow(maiDf)
# [1] 931942

# take mean of each parameter for each station and each year
janMeanDf <- SparkR::agg(SparkR::group_by(janDf,"STATIONS_ID","YEAR"), allMeanREGENj = mean(janDf$NIEDERSCHLAGSHOEHE), allMeanSCHNEEj = mean(janDf$SCHNEEHOEHE))
maiMeanDf <- SparkR::agg(SparkR::group_by(maiDf,"STATIONS_ID","YEAR"), allMeanREGENm = mean(maiDf$NIEDERSCHLAGSHOEHE), allMeanSCHNEEm = mean(maiDf$SCHNEEHOEHE))

janMeanDf <- SparkR::withColumnRenamed(janMeanDf, "STATIONS_ID", "SID")
janMeanDf <- SparkR::withColumnRenamed(janMeanDf, "YEAR", "YEAR")
maiMeanDf <- SparkR::withColumnRenamed(maiMeanDf, "STATIONS_ID", "SID_M")
maiMeanDf <- SparkR::withColumnRenamed(maiMeanDf, "YEAR", "YEAR_M")

# join jan and mai meanDfs
joinedDfjm<- SparkR::join(janMeanDf, maiMeanDf, janMeanDf$SID == maiMeanDf$SID_M & janMeanDf$YEAR == maiMeanDf$YEAR_M)
nrow(joinedDfjm)

# select needed columns
joinedDfjm_final <-  SparkR::select(joinedDfjm,"SID", "YEAR", "allMeanREGENj", "allMeanSCHNEEj","allMeanREGENm", "allMeanSCHNEEm" )
SparkR::head(joinedDfjm_final)

# save joinedDfjm for plotting
SparkR::write.df(joinedDfjm_final, "tropfen_januar_schnee_mai", "com.databricks.spark.csv", "overwrite")

output <- SparkR:::collect(joinedDfjm_final)
outfile = "Outputs/intermediateCSV/tropfen_januar_schnee_mai.csv"
write.csv(output,outfile,row.names=FALSE)
