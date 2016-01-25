morePrecipSchema <- structType(
  structField("STATIONS_ID", "integer"),
  structField("MESS_DATUM", "date"),
  structField("QUALITAETS_NIVEAU", "double"),
  structField("NIEDERSCHLAGSHOEHE","double"),
  structField("NIEDERSCHLAGSHOEHE_IND","double"),
  structField("SCHNEEHOEHE","double")
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

metaDf <- read.df(sqlContext,
                  "/home/dhaeb/dvl/data/dwd/more_precip/RR_Tageswerte_Beschreibung_Stationen2.txt",
                  "com.databricks.spark.csv", 
                  metaSchema, 
                  header="true", delimiter = "\t")

cache(metaDf)

morePrecipDf <- read.df(sqlContext,
                        "/home/dhaeb/dvl/data/dwd/more_precip/moreprecip_total.csv",
                        #"/home/dhaeb/dvl/data/dwd/more_precip/tageswerte_RR_00001_19120101_19860630_hist/produkt_klima_Tageswerte_19120101_19860630_00001.txt",
                        "com.databricks.spark.csv", 
                        morePrecipSchema, 
                        header="true", delimiter = ";")
cache(morePrecipDf)

morePrecipDf$MONTH <- month(morePrecipDf$MESS_DATUM)
morePrecipDf$YEAR <- year(morePrecipDf$MESS_DATUM)

# Mache aus Tagesdaten Monatsdaten - Summiere Niederschläge pro Monat aus Tagesdaten

totalNiedPerMonth <- agg(group_by(morePrecipDf, morePrecipDf$STATIONS_ID,morePrecipDf$YEAR,morePrecipDf$MONTH), 
                         sumNied = sum(morePrecipDf$NIEDERSCHLAGSHOEHE))
cache(totalNiedPerMonth)

totalNiedPerYear <- agg(group_by(totalNiedPerMonth, totalNiedPerMonth$STATIONS_ID,totalNiedPerMonth$YEAR), 
                        sumNiedPerYear = sum(totalNiedPerMonth$sumNied))

totalNiedMean <- agg(group_by(totalNiedPerYear, totalNiedPerYear$STATIONS_ID), 
                     totalNiedMean = mean(totalNiedPerYear$sumNiedPerYear))

totalNiedMean <- select(totalNiedMean, SparkR::alias(totalNiedMean$STATIONS_ID, "SID"),
                        totalNiedMean$totalNiedMean)
cache(totalNiedMean)

totalNiedInFeb <- where(totalNiedPerMonth, totalNiedPerMonth$MONTH == 2)

totalNiedInFebMean <- agg(group_by(totalNiedInFeb, totalNiedInFeb$STATIONS_ID), 
                          totalNiedMeanFeb = mean(totalNiedInFeb$sumNied))

totalNiedInFeb <- select(totalNiedInFeb, SparkR::alias(totalNiedInFeb$STATIONS_ID, "SID"), 
                         SparkR::alias(totalNiedInFeb$YEAR, "YEAR_FEB"), 
                         SparkR::alias(totalNiedInFeb$sumNied, "sumNiedFeb"))

cache(totalNiedInFeb)

joinedDataFeb <- join(totalNiedInFeb, totalNiedInFebMean,
                      totalNiedInFeb$SID == totalNiedInFebMean$STATIONS_ID)

joinedDataFeb <- select(joinedDataFeb, joinedDataFeb$SID, joinedDataFeb$YEAR_FEB, joinedDataFeb$sumNiedFeb > joinedDataFeb$totalNiedMeanFeb)

cache(joinedDataFeb)

jointedDataYear <- join(totalNiedPerYear, totalNiedMean, totalNiedPerYear$STATIONS_ID == totalNiedMean$SID)

jointedDataYear <- select(jointedDataYear, jointedDataYear$STATIONS_ID, jointedDataYear$YEAR, jointedDataYear$sumNiedPerYear > jointedDataYear$totalNiedMean)

cache(jointedDataYear)

joinedData <- join(jointedDataYear, joinedDataFeb, jointedDataYear$YEAR == joinedDataFeb$YEAR_FEB & jointedDataYear$STATIONS_ID == joinedDataFeb$SID)

overMean <- where(joinedData, joinedData$"(sumNiedPerYear > totalNiedMean)" & joinedData$"(sumNiedFeb > totalNiedMeanFeb)")

cache(joinedData)

cAll <- agg(group_by(joinedData, joinedData$STATIONS_ID), cAll = n(joinedData$YEAR))
cOverMean <- agg(group_by(overMean, overMean$SID), cOverMean = n(overMean$YEAR))

cache(cAll)
cache(cOverMean)

a <- count(overMean)
b <- count(joinedData)
meanAll <-   a / b

cFinal <- join(cAll, cOverMean, cAll$STATIONS_ID == cOverMean$SID)
cFinal <- select(cFinal, cFinal$STATIONS_ID, cFinal$cAll, cFinal$cOverMean, cFinal$cOverMean / cFinal$cAll)
cFinal <- join(cFinal, metaDf, cFinal$STATIONS_ID == metaDf$STATIONS_ID)
cFinal <- arrange(cFinal, desc(cFinal$"(cOverMean / cAll)"))
cache(cFinal)
exportCsv <- select(cFinal, cFinal$cAll, cFinal$cOverMean, SparkR::alias(cFinal$"(cOverMean / cAll)", "Verhältnis"), cFinal$longitude, cFinal$latitude, cFinal$Stationsname, cFinal$Bundesland, cFinal$Lage, cFinal$Statationshoehe, cFinal$von_datum, cFinal$bis_datum)
write.df(exportCsv, "wetter.csv", "com.databricks.spark.csv", "overwrite")
