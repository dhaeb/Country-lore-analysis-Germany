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

metaSchema <- structType(
  structField("STATIONS_ID", "integer"),
  structField("von_datum", "string"),
  structField("bis_datum", "string"),
  structField("Statationshoehe", "double"),
  structField("longitude", "double"),
  structField("latitude", "double"),
  structField("Stationsname", "string"),
  structField("Bundesland", "string"),
  structField("Lage", "string")
)

metaDfSoil <- read.df(sqlContext,
                  "/home/dhaeb/dvl/data/dwd/soil_temperature/EB_Tageswerte_Beschreibung_Stationen2.txt",
                  "com.databricks.spark.csv", 
                  metaSchema, 
                  header="true", delimiter = "\t")

# metaDfSoil <- select(metaDfSoil, 
#                      SparkR::alias(metaDfSoil$STATIONS_ID, "ID"), 
#                      SparkR::alias(metaDfSoil$"von_datum", "vd"),
#                      SparkR::alias(metaDfSoil$"bis_datum", "bd"),
#                      SparkR::alias(metaDfSoil$Statationshoehe, "sh"),
#                      SparkR::alias(metaDfSoil$longitude, "long"),
#                      SparkR::alias(metaDfSoil$latitude, "lat"),
#                      SparkR::alias(metaDfSoil$Bundesland, "bl"),
#                      SparkR::alias(metaDfSoil$Lage, "la"))
                     
metaDf <- read.df(sqlContext,
                  "/home/dhaeb/dvl/data/dwd/kl/KL_Tageswerte_Beschreibung_Stationen4.txt",
                  "com.databricks.spark.csv", 
                  metaSchema, 
                  header="true", delimiter = "\t")

# metaDf2 <- join(metaDf, metaDfSoil, 
#                metaDf$STATIONS_ID == metaDfSoil$ID &
#                metaDf$von_datum == metaDfSoil$vd &
#                metaDf$bis_datum == metaDfSoil$bd &
#                metaDf$Statationshoehe == metaDfSoil$sh &
#                metaDf$longitude == metaDfSoil$long &
#                metaDf$latitude == metaDfSoil$lat &
#                metaDf$Bundesland == metaDfSoil$bl &
#                metaDf$Lage == metaDfSoil$la 
#               , "outer")

cache(metaDf)

frostTempSpaltenName <- "LUFTTEMP_AM_ERDB_MINIMUM"

soilSchema <- structType(
  structField("STATIONS_ID", "integer"),
  structField("MESS_DATUM", "date"),
  structField("QUALITAETS_NIVEAU", "double"),
  structField("ERDBODENTEMPERATUR1", "double"),
  structField("MESS_TIEFE1", "double"),
  structField("ERDBODENTEMPERATUR2", "double"),
  structField("MESS_TIEFE2", "double"),
  structField("ERDBODENTEMPERATUR3", "double"),
  structField("MESS_TIEFE3", "double"),
  structField("ERDBODENTEMPERATUR4", "double"),
  structField("MESS_TIEFE4", "double"),
  structField("ERDBODENTEMPERATUR5", "double"),
  structField("MESS_TIEFE5", "double")
)

frostColumnName <- "ERDBODENTEMPERATUR5"
messTiefeColumnName <- "MESS_TIEFE5"

soilDf <- read.df(sqlContext,
                "/home/dhaeb/dvl/data/dwd/soil_temperature/soil_total.csv",
                "com.databricks.spark.csv", 
                soilSchema, 
                header="true", delimiter = ";")

soilDf <- select(where(soilDf, soilDf[[frostColumnName]] != -999 | soilDf[[messTiefeColumnName]] != -999), 
                 soilDf[[frostColumnName]], soilDf[[messTiefeColumnName]], SparkR::alias(soilDf$"STATIONS_ID", "SOIL_SID"), soilDf$"MESS_DATUM")
soilDf$M <- month(soilDf$MESS_DATUM)
soilDf$Y <- year(soilDf$MESS_DATUM)
soilDf$D <- dayofmonth(soilDf$MESS_DATUM)

klDf <- read.df(sqlContext,
                        "/home/dhaeb/dvl/data/dwd/kl/kl_total.csv",
                        #klDf"/home/dhaeb/dvl/data/dwd/more_precip/tageswerte_RR_00001_19120101_19860630_hist/produkt_klima_Tageswerte_19120101_19860630_00001.txt",
                        "com.databricks.spark.csv", 
                        klSchema, 
                        header="true", delimiter = ";")
klDf <- select(where(klDf, klDf$"NIEDERSCHLAGSHOEHE" != -999 | klDf$"LUFTTEMP_AM_ERDB_MINIMUM" != -999), 
                     "NIEDERSCHLAGSHOEHE", frostTempSpaltenName,"STATIONS_ID","MESS_DATUM")
klDf$MONTH <- month(klDf$MESS_DATUM)
klDf$YEAR <- year(klDf$MESS_DATUM)
klDf$DAY <- dayofmonth(klDf$MESS_DATUM)

klDf <- join(klDf, soilDf, klDf$STATIONS_ID == soilDf$SOIL_SID &
                           klDf$YEAR == soilDf$Y &
                           klDf$MONTH == soilDf$M & 
                           klDf$DAY == soilDf$D)

cache(klDf)
# Mache aus Tagesdaten Monatsdaten - Summiere Niederschläge pro Monat aus Tagesdaten
# Betrachte nur Datensätze, welche im Januar Niederschläge aufweisen können
cleanedKlNied <- where(klDf, klDf$NIEDERSCHLAGSHOEHE > 0 & klDf$MONTH == 1)

# Zähle pro StationsID die Anzahl der Jahre
cleanedKlNiedCountYears <- agg(group_by(distinct(select(klDf, klDf$YEAR, klDf$STATIONS_ID)), klDf$STATIONS_ID),
                               countAllYears = count(klDf$STATIONS_ID))
cleanedKlNiedCountYears <- select(cleanedKlNiedCountYears, SparkR:::alias(cleanedKlNiedCountYears$STATIONS_ID,"S_ID"), cleanedKlNiedCountYears$countAllYears)
# head(cleanedKl)
# NIEDERSCHLAGSHOEHE LUFTTEMP_AM_ERDB_MINIMUM STATIONS_ID MESS_DATUM MONTH YEAR
# 1                3.3                      3.3        6337 2005-01-01     1 2005
# 2                0.1                      2.4        6337 2005-01-02     1 2005
# 3                0.1                      2.6        6337 2005-01-03     1 2005
# 4                0.9                      4.7        6337 2005-01-04     1 2005
# 5                3.6                      4.5        6337 2005-01-05     1 2005
# 6                0.1                      3.9        6337 2005-01-08     1 2005

totalNiedPerMonth <- agg(group_by(cleanedKlNied, cleanedKlNied$STATIONS_ID, cleanedKlNied$YEAR),
                         countAllRainDaysPerMonth = count(cleanedKlNied$STATIONS_ID))
# count(totalNiedPerMonth) --> 29922
cache(totalNiedPerMonth)

totalNiedPerYear <- agg(group_by(totalNiedPerMonth, totalNiedPerMonth$STATIONS_ID),
                         countAllRainDays = sum(totalNiedPerMonth$countAllRainDaysPerMonth))
cache(totalNiedPerYear)

# Zähle pro StationsID die Anzahl der Jahre
cleanedKlNiedCountYears <- agg(group_by(distinct(select(klDf, klDf$YEAR, klDf$STATIONS_ID)), klDf$STATIONS_ID),
                               countAllYears = count(klDf$STATIONS_ID))
cleanedKlNiedCountYears <- select(cleanedKlNiedCountYears, SparkR:::alias(cleanedKlNiedCountYears$STATIONS_ID,"S_ID"), cleanedKlNiedCountYears$countAllYears)

# berechne durschnittliches Regenaufkommen über alle Jahre hinweg
countRainDaysJoinedWithYearCount <- join(totalNiedPerYear, cleanedKlNiedCountYears, totalNiedPerMonth$STATIONS_ID == cleanedKlNiedCountYears$S_ID)
countRainDaysJoinedWithYearCount$relRainInJan <- countRainDaysJoinedWithYearCount$countAllRainDays / countRainDaysJoinedWithYearCount$countAllYears
countRainDaysJoinedWithYearCount  <- select(countRainDaysJoinedWithYearCount, "S_ID", "countAllRainDays", "countAllYears", "relRainInJan")
# count(countRainDaysJoinedWithYearCount) --> 964
cache(countRainDaysJoinedWithYearCount)
#head(countRainDaysJoinedWithYearCount)

joindedNiedFinal <- select(join(countRainDaysJoinedWithYearCount, totalNiedPerMonth, totalNiedPerMonth$STATIONS_ID == countRainDaysJoinedWithYearCount$S_ID), "S_ID", "YEAR", "countAllYears", "countAllRainDays", "relRainInJan", "countAllRainDaysPerMonth") 
cache(joindedNiedFinal)
# count(joindedNiedFinal) --> 28844808???
# S_ID YEAR countAllYears countAllRainDays relRainInJan
# 1  231 1987            39              634     16.25641
# 2  231 1976            39              634     16.25641
# 3  231 2000            39              634     16.25641
# 4  231 1990            39              634     16.25641
# 5  231 1979            39              634     16.25641
# 6  231 1970            39              634     16.25641
# countAllRainDaysPerMonth
# 1                       19
# 2                       24
# 3                       17
# 4                       15
# 5                       15
# 6                       14


frostColumnName <- "LUFTTEMP_AM_ERDB_MINIMUM"

# Frost im November:
#   Wie oft gab es Frost im November?
cleanedKlFrost <- where(klDf, klDf$MONTH == 11 & klDf[[frostColumnName]] < 0 & klDf$DAY <= 10)
cleanedKlFrost <- agg(group_by(cleanedKlFrost, cleanedKlFrost$STATIONS_ID, cleanedKlFrost$YEAR), 
                       countFrost = count(cleanedKlFrost[[frostColumnName]]))
cleanedKlFrost <- select(cleanedKlFrost, SparkR:::alias(cleanedKlFrost$STATIONS_ID, "SID"), SparkR:::alias(cleanedKlFrost$YEAR, "Y"), cleanedKlFrost$countFrost)
# Finde alle Jahre, in dem es kein Frost im November gab
# cleanedKlNotFrost <- distinct(select(klDf, SparkR:::alias(klDf$STATIONS_ID, "SID"), SparkR:::alias(klDf$YEAR, "Y")))
#cleanedKlNotFrost <- distinct(select(klDf, SparkR:::alias(klDf$STATIONS_ID, "SID"), SparkR:::alias(klDf$YEAR, "Y")))


# cleanedKlTotal <- select(join(cleanedKlFrost, cleanedKlNotFrost, cleanedKlNotFrost$SID == cleanedKlFrost$STATIONS_ID & cleanedKlNotFrost$Y == cleanedKlFrost$YEAR, "right_outer"), "SID", "Y", "countFrost")

# cache(cleanedKlTotal)

joinedFinal <- join(cleanedKlFrost, joindedNiedFinal, cleanedKlFrost$SID == joindedNiedFinal$S_ID & (cleanedKlFrost$Y - 1) == joindedNiedFinal$YEAR)

cache(joinedFinal)
SparkR::head(joinedFinal)

countAllCases <- function(df, myColName){
  countable <- agg(groupBy(df, df$SID), myColName = count(df$SID))
  return(countable)
}

datasetFullfillingRule <- SparkR::filter(joinedFinal, joinedFinal$countAllRainDaysPerMonth > joinedFinal$relRainInJan)
datasetNotFullfillingRule <- SparkR::filter(joinedFinal, joinedFinal$countAllRainDaysPerMonth < joinedFinal$relRainInJan)

calcTotal <- function(countableFulllilling, countableNotFullfilling) {
  countFullfilling <- count(countableFulllilling)
  total <- countFullfilling + count(countableNotFullfilling)
  print(total)
  return(countFullfilling / total)
} 

datasetFullfillingRule <- agg(groupBy(datasetFullfillingRule, datasetFullfillingRule$SID), countFullfillingRule = count(datasetFullfillingRule$SID))
datasetFullfillingRule <- select(datasetFullfillingRule, SparkR:::alias(datasetFullfillingRule$SID, "S_ID"), datasetFullfillingRule$countFullfillingRule)
datasetNotFullfillingRule <- agg(groupBy(datasetNotFullfillingRule, datasetNotFullfillingRule$SID), countNotFullfillingRule = count(datasetNotFullfillingRule$SID))
result <- select(join(datasetFullfillingRule, datasetNotFullfillingRule, datasetFullfillingRule$S_ID == datasetNotFullfillingRule$SID), "SID", "countFullfillingRule", "countNotFullfillingRule")
result$totalYearsWithFrost <- result$countFullfillingRule + result$countNotFullfillingRule
result$rel <- result$countFullfillingRule / result$totalYearsWithFrost
cFinal <- join(result, metaDf, result$SID == metaDf$STATIONS_ID)
cache(cFinal)
exportCsv <- select(cFinal, cFinal$totalYearsWithFrost, cFinal$countFullfillingRule, cFinal$rel, cFinal$longitude, cFinal$latitude, cFinal$Stationsname, cFinal$Bundesland, cFinal$Lage, cFinal$Statationshoehe, cFinal$von_datum, cFinal$bis_datum)
write.df(exportCsv, "nov_frost_jan_feucht", "com.databricks.spark.csv", "overwrite")

print(calcTotal(datasetFullfillingRule, datasetNotFullfillingRule))