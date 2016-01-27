stationDf <- read.df(sqlContext,
                     "/Users/martinmac/Big_Data_Prak_Lani/data/kl/KL_Tageswerte_Beschreibung_Stationen4.txt",
                     "com.databricks.spark.csv",
                     metaSchema,
                     header="true", delimiter = "\t")
SparkR::head(stationDf)
write.df(stationDf, "/Users/martinmac/Big_Data_Prak_Lani/stationDf", "com.databricks.spark.csv", "overwrite")
