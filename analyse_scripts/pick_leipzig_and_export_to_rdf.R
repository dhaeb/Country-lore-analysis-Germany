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

klDf <- read.df(sqlContext,
                        "/home/dhaeb/dvl/data/dwd/kl/kl_total.csv",
                        #klDf"/home/dhaeb/dvl/data/dwd/more_precip/tageswerte_RR_00001_19120101_19860630_hist/produkt_klima_Tageswerte_19120101_19860630_00001.txt",
                        "com.databricks.spark.csv", 
                        klSchema, 
                        header="true", delimiter = ";")
                        # Leipzig Holzhause + Leipizg/Halle
klDf <- where(klDf, klDf$STATIONS_ID == 2928 | klDf$STATIONS_ID == 2932)
                        
                        
columns <- list("NIEDERSCHLAGSHOEHE", "LUFTTEMPERATUR_MINIMUM", "LUFTTEMPERATUR_MAXIMUM")                        

filterString <- paste(paste(columns, " != -999.0"), collapse = " AND ")
klDf <- SparkR::filter(klDf, filterString)

# args for select
args <- append(columns, list("STATIONS_ID", "MESS_DATUM"))
args <- Map(function(e){return(klDf[[e]])}, args)
args <- append(list(klDf), args)
klDf <- do.call("select", args) # select execution with args 

# create additional columns for easier time extraction
klDf$MONTH <- month(klDf$MESS_DATUM)
klDf$YEAR <- year(klDf$MESS_DATUM)
klDf$DAY <- dayofmonth(klDf$MESS_DATUM)

metaSchema <- structType(
  structField("SID", "integer"),
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
                  
finalDf <- join(metaDf, klDf, metaDf$SID == klDf$STATIONS_ID)
finalRDf <- SparkR::collect(finalDf)

write.csv(finalRDf, file = "./leipzig-Rdf.csv")