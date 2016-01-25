# Wie's Wetter am Siebenschläfertag, so bleibt es sieben Wochen danach.
# parameter1: sonne - SONNENSCHEINDAUER (26.06 - 28.06 vs 29.06 - 17.08)
# parameter2: regen - NIEDERSCHLAGSHOEHE (26.06 - 28.06 vs 29.06 - 17.08)
# parameter3: temp - LUFTTEMPERATUR (26.06 - 28.06 vs 29.06 - 17.08)

# a case:= specific station in specific year

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
                path = "/Users/martinmac/Big_Data_Prak_Lani/data/kl/kl_total.csv",
                source = "com.databricks.spark.csv",
                schema = klSchema,
                header="true", delimiter = ";")
# select needed columns
inDf1 <- select(inDf1, "STATIONS_ID","MESS_DATUM","QUALITAETS_NIVEAU","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
# nrow(inDf)
# [1] 12877275

### clean data ###
# TODO: take data with QUALITAETS_NIVEAU == 10 & 5 ?

# remove values = -999
inDf1 <- select(where(inDf1, inDf1$SONNENSCHEINDAUER != -999),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
inDf1 <- select(where(inDf1, inDf1$NIEDERSCHLAGSHOEHE != -999),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
inDf1 <- select(where(inDf1, inDf1$LUFTTEMPERATUR != -999 ), "STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")

# select needed rows
# inDf1$DATEYEAR <- dayofyear(inDf1$MESS_DATUM)
inDf1$DATE <- dayofmonth(inDf1$MESS_DATUM)
inDf1$MONTH <- month(inDf1$MESS_DATUM)
inDf1$YEAR <- year(inDf1$MESS_DATUM)
take(inDf1, 5L)
# STATIONS_ID MESS_DATUM SONNENSCHEINDAUER NIEDERSCHLAGSHOEHE LUFTTEMPERATUR DATE MONTH YEAR
# 1        6337 2004-08-01              12.2                  0           18.8    1     8 2004
# 2        6337 2004-08-02              11.7                  0           20.0    2     8 2004
# 3        6337 2004-08-03              12.2                  0           21.3    3     8 2004
# 4        6337 2004-08-04               6.8                  0           21.4    4     8 2004
# 5        6337 2004-08-05              10.5                  0           24.1    5     8 2004

# nrow(inDf1)
# [1] 6283341

juniDf <- select(where(inDf1, inDf1$MONTH == 6),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR","DATE","MONTH","YEAR")
julyDf <- select(where(inDf1, inDf1$MONTH == 7),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR","DATE","MONTH","YEAR")
augustDf <- select(where(inDf1, inDf1$MONTH == 8),"STATIONS_ID","MESS_DATUM","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR","DATE","MONTH","YEAR")
take(juniDf,5L)
# STATIONS_ID MESS_DATUM SONNENSCHEINDAUER NIEDERSCHLAGSHOEHE LUFTTEMPERATUR DATE MONTH YEAR
# 1        6337 2005-06-01              11.1                0.3           11.3    1     6 2005
# 2        6337 2005-06-02               1.0                0.0           15.3    2     6 2005
# 3        6337 2005-06-03              10.3                0.2           19.2    3     6 2005
# 4        6337 2005-06-04               3.2                6.1           14.0    4     6 2005
# 5        6337 2005-06-05               1.1               13.5           13.6    5     6 2005

tageDf <- select(where(juniDf, juniDf$DATE > 25 & juniDf$DATE < 29 ),"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
juniWDf <- select(where(juniDf, juniDf$DATE > 28),"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
julyWDf <- select(julyDf,"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
augustWDf <- select(where(augustDf, augustDf$DATE < 18),"STATIONS_ID","YEAR","SONNENSCHEINDAUER","NIEDERSCHLAGSHOEHE","LUFTTEMPERATUR")
wochenDf <- rbind(juniWDf,julyWDf,augustWDf)

nrow(tageDf)
# [1] 51567

nrow(wochenDf)
# [1] 860685

take(tageDf,5L)
# STATIONS_ID YEAR SONNENSCHEINDAUER NIEDERSCHLAGSHOEHE LUFTTEMPERATUR
# 1        6337 2005              10.5                0.0           17.0
# 2        6337 2005              14.7                0.0           18.5
# 3        6337 2005              15.1                2.3           18.6
# 4        6337 2006               5.5                0.0           18.9
# 5        6337 2006               0.5                0.0           15.6

take(wochenDf,5L)
# STATIONS_ID YEAR SONNENSCHEINDAUER NIEDERSCHLAGSHOEHE LUFTTEMPERATUR
# 1        6337 2005               4.2                1.5           17.9
# 2        6337 2005               1.6               10.0           17.4
# 3        6337 2006              14.0                0.0           18.0
# 4        6337 2006              14.8                0.0           20.2
# 5        6337 2007               0.8               13.7           14.5


# take mean of each parameter for each station and each year
tageMeanDf <- agg(group_by(tageDf,"STATIONS_ID","YEAR"), allMeanSONNE = mean(tageDf$SONNENSCHEINDAUER), allMeanREGEN = mean(tageDf$NIEDERSCHLAGSHOEHE), allMeanTEMP = mean(tageDf$LUFTTEMPERATUR))
wochenMeanDf <- agg(group_by(wochenDf,"STATIONS_ID","YEAR"), allMeanSONNEw = mean(wochenDf$SONNENSCHEINDAUER), allMeanREGENw = mean(wochenDf$NIEDERSCHLAGSHOEHE), allMeanTEMPw = mean(wochenDf$LUFTTEMPERATUR))

tageMeanDf <- withColumnRenamed(tageMeanDf, "STATIONS_ID", "SID")
tageMeanDf <- withColumnRenamed(tageMeanDf, "YEAR", "YEAR")
wochenMeanDf <- withColumnRenamed(wochenMeanDf, "STATIONS_ID", "SID_W")
wochenMeanDf <- withColumnRenamed(wochenMeanDf, "YEAR", "YEAR_W")

# join tage and wochen meanDfs
joinedDf <- join(tageMeanDf, wochenMeanDf, tageMeanDf$SID == wochenMeanDf$SID_W & tageMeanDf$YEAR == wochenMeanDf$YEAR_W ,"left_outer")
nrow(joinedDf)
# [1] 17243

# remove NA
df7 <- dropna(joinedDf)
nrow(df7)
# [1] 17243

take(joinedDf,5L)
# SID YEAR allMeanSONNE allMeanREGEN allMeanTEMP SID_W YEAR_W allMeanSONNEw allMeanREGENw
# 1 150 1997     7.000000   1.60000000    14.76667   150   1997         7.998         1.704
# 2 183 1976    14.833333   0.00000000    21.16667   183   1976         9.962         0.646
# 3 232 1963     8.333333   0.03333333    18.46667   232   1963         8.844         3.250
# 4 259 1964     9.566667   0.03333333    20.03333   259   1964         9.254         1.302
# 5 480 1987     5.166667   2.36666667    16.53333   480   1987         5.800         2.206
# allMeanTEMPw
# 1       19.346
# 2       17.598
# 3       18.330
# 4       19.768
# 5       15.736

# correlation: for each station, tag7 vs wochen7 through all years
###### correlation is not supported in sparkR < 1.6

# stimmt in Deutschland?
# positive correlation
## calcualte: # of case with correlations > 0.7 / allCases

##  calcualte: # of case with correlations > 0.9 / allCases

# negative correlation
##  calcualte: # of case with correlations < -0.7 / allCases

##  calcualte: # of case with correlations > -0.9 / allCases
