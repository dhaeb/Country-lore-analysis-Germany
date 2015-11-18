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

metaDf <- read.df(sqlContext,
                  "/home/dhaeb/dvl/data/dwd/kl/KL_Tageswerte_Beschreibung_Stationen4.txt",
                  "com.databricks.spark.csv", 
                  metaSchema, 
                  header="true", delimiter = "\t")

mySt <- read.df(sqlContext,
              #"/home/dhaeb/dvl/data/dwd/kl/kl_total.csv",
              "/home/dhaeb/dvl/data/dwd/kl/tageswerte_00001_19370101_19860630_hist/produkt_klima_Tageswerte_19370101_19860630_00001_ndf.txt",
              "com.databricks.spark.csv", 
              klSchema, 
              header="true", delimiter = ";")

joinedDf <- join(metaDf, mySt, metaDf$STATIONS_ID == mySt$STATIONS_ID)

mySt$MONTH <- month(mySt$MESS_DATUM)
mySt$YEAR <- year(mySt$MESS_DATUM)

a <- agg(group_by(mySt, "STATIONS_ID"), minDatum = min(mySt$MESS_DATUM), maxDatum = max(mySt$MESS_DATUM), meanLTM = mean(mySt$LUFTTEMPERATUR_MAXIMUM))


novTemp <- select(where(mySt, mySt$MONTH == 11), mySt$LUFTTEMPERATUR_MINIMUM)#, mySt$YEAR)
janNied <- select(where(mySt, mySt$MONTH == 1), mySt$NIEDERSCHLAGSHOEHE)#, mySt$YEAR)

minData <- min(count(novTemp), count(janNied)) / 30

nov <- collect(novTemp)
jan <- collect(janNied)

liste <- vector()

for(i in 1:minData){
  startNov <- 31 * (i - 1) + 1
  endNov <- startNov + 30
  startJan <- 32 + (i - 1) * (32) 
  endJan <- startJan + 30
  #print(c(startNov, endNov, startJan, endJan))
  liste[i] = cor(nov$LUFTTEMPERATUR_MINIMUM[startNov:endNov], jan$NIEDERSCHLAGSHOEHE[startJan:endJan], method = "spearman")
}
print(mean(liste, na.rm = TRUE))

