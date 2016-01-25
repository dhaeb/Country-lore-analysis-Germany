#
# Depedencies to run first: init-sparkr.R schemata.R
#

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

clInputOktoberWarmFein <- list(
  pathToMeta = "/home/dhaeb/dvl/data/dwd/kl/KL_Tageswerte_Beschreibung_Stationen4.txt",
  pathToData = "/home/dhaeb/dvl/data/dwd/kl/kl_total.csv",
  schema = klSchema,
  triggerCols = c("LUFTTEMPERATUR_MAXIMUM", "SONNENSCHEINDAUER"),
  triggerTimeFilter = "MONTH = 10",
  triggerExpressionFilter = function(df){
    return(df$LUFTTEMPERATUR_MAXIMUM_AGG_MONTH_TRIG > df$LUFTTEMPERATUR_MAXIMUM_AGG_TOTAL_TRIG & df$SONNENSCHEINDAUER_AGG_MONTH_TRIG > df$SONNENSCHEINDAUER_AGG_TOTAL_TRIG)
  },
  observationCols = c("LUFTTEMPERATUR_MINIMUM"),
  observationTimeFilter = "MONTH = 1 or MONTH = 2",
  observationExpression = function(df){
    return(df$LUFTTEMPERATUR_MINIMUM_AGG_MONTH_OBS < df$LUFTTEMPERATUR_MINIMUM_AGG_TOTAL_OBS)
  },
  joinExpression = function(triggerTotalDf, observationTotalDf){ 
    return(triggerTotalDf$STATIONS_ID == observationTotalDf$SID & triggerTotalDf$YEAR == (observationTotalDf$Y - 1))
  },
  folderName = "oktober_warm_fein"
)

clInputOktoberNassKühl <- list(
  pathToMeta = "/home/dhaeb/dvl/data/dwd/kl/KL_Tageswerte_Beschreibung_Stationen4.txt",
  pathToData = "/home/dhaeb/dvl/data/dwd/kl/kl_total.csv",
  schema = klSchema,
  triggerCols = c("LUFTTEMPERATUR_MINIMUM", "NIEDERSCHLAGSHOEHE"),
  triggerTimeFilter = "MONTH = 10",
  triggerExpressionFilter = function(df){
    return(df$LUFTTEMPERATUR_MINIMUM_AGG_MONTH_TRIG < df$LUFTTEMPERATUR_MINIMUM_AGG_TOTAL_TRIG & df$NIEDERSCHLAGSHOEHE_AGG_MONTH_TRIG > df$NIEDERSCHLAGSHOEHE_AGG_TOTAL_TRIG)
  },
  observationCols = c("LUFTTEMPERATUR_MAXIMUM"),
  observationTimeFilter = "MONTH = 1 or MONTH = 2",
  observationExpression = function(df){
    return(df$LUFTTEMPERATUR_MAXIMUM_AGG_MONTH_OBS > df$LUFTTEMPERATUR_MAXIMUM_AGG_TOTAL_OBS)
  },
  joinExpression = function(triggerTotalDf, observationTotalDf){ 
    return(triggerTotalDf$STATIONS_ID == observationTotalDf$SID & triggerTotalDf$YEAR == (observationTotalDf$Y - 1))
  },
  folderName = "oktober_nass_kuehl"
)

clInputFebruarNassJahrNass <- list(
  pathToMeta = "/home/dhaeb/dvl/data/dwd/more_precip/RR_Tageswerte_Beschreibung_Stationen2.txt",
  pathToData = "/home/dhaeb/dvl/data/dwd/more_precip/moreprecip_total.csv",
  schema = morePrecipSchema,
  triggerCols = c("NIEDERSCHLAGSHOEHE"),
  triggerTimeFilter = "MONTH = 2",
  triggerExpressionFilter = function(df){
    return(df$NIEDERSCHLAGSHOEHE_AGG_MONTH_TRIG > df$NIEDERSCHLAGSHOEHE_AGG_TOTAL_TRIG)
  },
  observationCols = c("NIEDERSCHLAGSHOEHE"),
  observationTimeFilter = "",
  observationExpression = function(df){
    return(df$NIEDERSCHLAGSHOEHE_AGG_MONTH_OBS > df$NIEDERSCHLAGSHOEHE_AGG_TOTAL_OBS)
  },
  joinExpression = function(triggerTotalDf, observationTotalDf){ 
    return(triggerTotalDf$STATIONS_ID == observationTotalDf$SID & triggerTotalDf$YEAR == observationTotalDf$Y)
  },
  folderName = "feb_nass_jahr_nass"
)

aggregateByStatIdAndYear <- function(df, colsNames, aggColSuffix) {
  #   build agg call for do call because the list of input columns is variable
  argListGroupByPart <- list(groupBy(df, df$STATIONS_ID, df$YEAR))  
  #   extract colnames to list
  colNameList <- list()
  for(colName in colsNames){
    aggColNameMonth <- paste(colName, aggColSuffix, sep="")
    colNameList[aggColNameMonth] <- df[[colName]]
  }
  #   add mean call for agg
  groupedColumns <- lapply(colNameList, function(e){
    return(mean(e))
  })
  #   append groupBy and aggregation arguments of agg 
  a <- append(argListGroupByPart, groupedColumns)
  #   call agg
  dfColAggMonth <- do.call("agg", a)
  #   get col names again and transform them to a column identifier (for select)
  cols <- lapply(names(colNameList), function(e){
    return(dfColAggMonth[[e]])
  })
  #   contruct arg list of add, just append STATION_ID and YEAR columns with the other aggregation cols
  selectArgList <- append(list(dfColAggMonth, dfColAggMonth$STATIONS_ID, dfColAggMonth$YEAR), cols)
  return(do.call("select", selectArgList))
}

aggregateByStatId <- function(df, colNames, joinable, aggColSuffix){
  for(colName in colNames){
    # calculate the aggregation over all years
    aggColName <- paste(colName, aggColSuffix, sep="")
    stationIdColName <- paste(colName, "_STATIONS_ID", sep="")
    dfColAgg <- agg(groupBy(df, df$STATIONS_ID), aggCol = mean(df[[colName]]))
    dfColAgg <- select(dfColAgg, SparkR::alias(dfColAgg$STATIONS_ID, stationIdColName), SparkR::alias(dfColAgg$aggCol, aggColName))
    joinable <- join(joinable, dfColAgg, joinable$STATIONS_ID == dfColAgg[[stationIdColName]])
  }
  return(joinable)
}

readMetaDf <- function(pathToMeta){
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
                    pathToMeta,
                    "com.databricks.spark.csv", 
                    metaSchema, 
                    header="true", delimiter = "\t")
  cache(metaDf)
  return(metaDf)
}

analyseCountryLore <- function(clInput){
  dataDf <- read.df(sqlContext,
                    clInput$pathToData,
                    "com.databricks.spark.csv", 
                    clInput$schema, 
                    header="true", delimiter = ";")
  cache(dataDf)
  allCols <- c(clInput$triggerCols, clInput$observationCols)
  
  # clean dataset
  dataDf <- where(dataDf, dataDf$QUALITAETS_NIVEAU > 3)
  for(col in allCols){
    dataDf <- where(dataDf, dataDf[[col]] != -999)
  }
  
  # introduce date columns
  dataDf$DAY <- dayofmonth(dataDf$MESS_DATUM)
  dataDf$MONTH <- month(dataDf$MESS_DATUM)
  dataDf$YEAR <- year(dataDf$MESS_DATUM)
  
  # join with others?
  
  # calculate
  #   trigger columns filter 
  triggerDf <- SparkR::filter(dataDf, clInput$triggerTimeFilter)
  countTotalCleaned <- count(triggerDf)
  
  
  #     columns aggregation by month (STATIONS_ID, YEAR)
  triggerDfColAggMonth <- aggregateByStatIdAndYear(triggerDf, clInput$triggerCols, "_AGG_MONTH_TRIG")
  #     trigger columns aggregation by month (STATIONS_ID)
  triggerDfColAggMonth <- aggregateByStatId(triggerDf, clInput$triggerCols, triggerDfColAggMonth, "_AGG_TOTAL_TRIG")
  #     just use data, which is related to the country lore
  triggerTotalDf <- SparkR::filter(triggerDfColAggMonth, clInput$triggerExpression(triggerDfColAggMonth))
  cache(triggerTotalDf)
  countFullfillingTriggeringRule <- count(triggerTotalDf)
  # print(SparkR::head(triggerDfColAggTotal))
  
  
  #   observation columns filter
  if(nchar(clInput$observationTimeFilter) == 0){
    observationDf <- dataDf
  }  else {
    observationDf <- SparkR::filter(dataDf, clInput$observationTimeFilter )
  }
  #     columns aggregation by month (STATIONS_ID, YEAR)
  observationDfColAggMonth <- aggregateByStatIdAndYear(observationDf, clInput$observationCols, "_AGG_MONTH_OBS")
  #     trigger columns aggregation by month (STATIONS_ID)
  observationTotalDf <- aggregateByStatId(observationDf, clInput$observationCols, observationDfColAggMonth, "_AGG_TOTAL_OBS")
  cache(observationTotalDf)
  
  # extract other columns using functional programming
  otherColumns <- Map(function(e){ 
      observationTotalDf[[e]]
    }, Filter(function(e){
          e != "STATIONS_ID" & e != "YEAR"
       }, names(observationTotalDf)))
  # rename STATION_ID and YEAR of observationTotalDf
  observationTotalDf <- do.call("select", append(list(observationTotalDf, SparkR::alias(observationTotalDf$STATIONS_ID, "SID"), SparkR::alias(observationTotalDf$YEAR, "Y")), otherColumns))

  #   print(countTotalCleaned)
  #   print(countFullfillingTriggeringRule)
    
  # join into final data frame to count the results
  countablePreresult <- join(triggerTotalDf, observationTotalDf, clInput$joinExpression(triggerTotalDf, observationTotalDf))
  cache(countablePreresult)
  countAllYearsPerStationFullfillingTrigger <- agg(groupBy(countablePreresult, countablePreresult$STATIONS_ID), countAll = count(countablePreresult$YEAR))
  countablePreresultFullfillingObservation <- SparkR::filter(countablePreresult, clInput$observationExpression(countablePreresult))
  countAllYearsPerStationFullfillingObservation <- agg(groupBy(countablePreresultFullfillingObservation, countablePreresult$SID), cFullfilling = count(countablePreresult$Y))
  totalResult <- join(countAllYearsPerStationFullfillingTrigger, countAllYearsPerStationFullfillingObservation, countAllYearsPerStationFullfillingTrigger$STATIONS_ID == countAllYearsPerStationFullfillingObservation$SID)  
  # calc relation
  totalResult$rel <- totalResult$cFullfilling / totalResult$countAll
  # read in meta schema
  metaDf <- readMetaDf(clInput$pathToMeta)
  # join with meta schema
  totalResult <- join(totalResult, metaDf, totalResult$SID == metaDf$STATIONS_ID)
  cache(totalResult)
  
  
  sparkRSum <- function(df, col){
    r <- collect(agg(groupBy(df), result = sum(col)))
    return(r[[1]])
  }
  
  # function calculating aggregated results
  aggregateResults <- function(createria){
    if(missing(createria)){
      df <- totalResult      
    } else {
      df <- SparkR::filter(totalResult, createria)  
    }
    return(sparkRSum(df, df$cFullfilling) / sparkRSum(df, df$countAll))
  }
  
  total <- aggregateResults()
  n <- aggregateResults(totalResult$Lage == "N")
  o <- aggregateResults(totalResult$Lage == "O")
  s <- aggregateResults(totalResult$Lage == "S")
  w <- aggregateResults(totalResult$Lage == "W")
  exportCsv <- select(totalResult, totalResult$countAll, totalResult$cFullfilling, totalResult$rel, totalResult$longitude, totalResult$latitude, totalResult$Stationsname, totalResult$Bundesland, totalResult$Lage, totalResult$Statationshoehe, totalResult$von_datum, totalResult$bis_datum)
  exportCsv <- orderBy(exportCsv, exportCsv$rel)
  write.df(exportCsv, clInput$folderName, "com.databricks.spark.csv", "overwrite")
  print(total)
  print(n)
  print(o)
  print(s)
  print(w)
  return(totalResult)
}

