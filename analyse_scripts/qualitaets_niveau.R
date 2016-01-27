# import data
mySt <- read.df(sqlContext,
                path = "/Users/martinmac/Big_Data_Prak_Lani/data/kl/kl_total.csv",
                source = "com.databricks.spark.csv",
                schema = klSchema,
                header="true", delimiter = ";")

# select needed columns
mySt <- select(mySt, "STATIONS_ID","MESS_DATUM","QUALITAETS_NIVEAU","SONNENSCHEINDAUER","SCHNEEHOEHE","LUFTTEMPERATUR")
# nrow(mySt)
# [1] 12877275
qualitat_count <- agg(group_by(mySt, "QUALITAETS_NIVEAU"), count = count(mySt$STATIONS_ID))
take(qualitat_count,5L)
# QUALITAETS_NIVEAU   count
# 1                 7    8308
# 2                 1  380068
# 3                 3 1052316
# 4                10 5133753
# 5                 5 6198086
# take data with QUALITAETS_NIVEAU == 10 ?
