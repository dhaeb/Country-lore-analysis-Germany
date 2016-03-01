#
# Depedencies to run first: init-sparkr.R schemata.R
#

# most used dataset schema

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

# helper function to join two related years together (for some rules like if the november contains soil frost, the next year january should be more rainy)
joinWithNextYear <- function(triggerTotalDf, observationTotalDf){ 
  return(triggerTotalDf$STATIONS_ID == observationTotalDf$SID & triggerTotalDf$YEAR == (observationTotalDf$Y - 1))
}

identity <- function(x) {
  return(x)
}

# helper function to create the input data set for an clr analysis
createClAnalysisDataset = function(folder, 
				   triggerObject,
				   observationObject,
				   joinExpression,
				   pathToMeta = "/home/dhaeb/dvl/data/dwd/kl/KL_Tageswerte_Beschreibung_Stationen4.txt",
				   pathToData = "/home/dhaeb/dvl/data/dwd/kl/kl_total.csv",
				   schema = klSchema,
				   triggerAggOperation = SparkR::mean,
				   observationAggOperation = SparkR::mean
				   ){
  return(list(
    pathToMeta = pathToMeta,
    pathToData = pathToData,
    schema = schema,
    triggerCols = triggerObject$triggerCols,
    triggerTimeFilter = triggerObject$triggerTimeFilter,
    triggerExpressionFilter = triggerObject$triggerExpressionFilter,
    triggerMap = triggerObject$triggerMap,
    triggerAggOperation = triggerAggOperation,
    observationCols = observationObject$observationCols,
    observationTimeFilter = observationObject$observationTimeFilter,
    observationExpression = observationObject$observationExpression,
    observationMap = observationObject$observationMap,
    observationAggOperation = observationAggOperation,
    joinExpression = joinExpression,
    folderName = folder
  ))
}

clInputJanHellSommerHeiss <- createClAnalysisDataset(
  "jan_hell_sommer_heiss",
  list(
    triggerCols = c("SONNENSCHEINDAUER", "SCHNEEHOEHE"),
    triggerTimeFilter = "MONTH = 1",
    triggerExpressionFilter = function(df){
      return(df$SONNENSCHEINDAUER_AGG_MONTH_TRIG > df$SONNENSCHEINDAUER_AGG_TOTAL_TRIG & df$SCHNEEHOEHE_AGG_MONTH_TRIG > 0)
    }
  ),
  list(
    observationCols = c("LUFTTEMPERATUR"),
    observationTimeFilter = "MONTH BETWEEN 6 AND 8",
    observationExpressionFilter = function(df){
      return(df$LUFTTEMPERATUR_AGG_MONTH_OBS > df$LUFTTEMPERATUR_AGG_TOTAL_OBS)
    }
  ),
  function(triggerTotalDf, observationTotalDf){ 
    return(triggerTotalDf$STATIONS_ID == observationTotalDf$SID & triggerTotalDf$YEAR == observationTotalDf$Y)
  }
)

clInputNovFrostJanNass <- createClAnalysisDataset(
  "nov_frost_jan_nass",
  list(
    triggerCols = c("LUFTTEMP_AM_ERDB_MINIMUM"),
    triggerTimeFilter = "MONTH = 11 and DAY BETWEEN 1 and 10 and LUFTTEMP_AM_ERDB_MINIMUM < 0",
    triggerMap = function(df){
      sidColNameYearsUsed <- "yearSID"
      yearsUsed <- select(df, "STATIONS_ID", "YEAR")
      yearsUsed <- agg(groupBy(yearsUsed, yearsUsed$STATIONS_ID), COUNT_YEARS_PER_SID = SparkR::countDistinct(yearsUsed$YEAR))
      yearsUsed <- select(yearsUsed, SparkR::alias(yearsUsed$STATIONS_ID, sidColNameYearsUsed ), yearsUsed$COUNT_YEARS_PER_SID)
      df <- join(df, yearsUsed, df$STATIONS_ID == yearsUsed[[sidColNameYearsUsed]])
      df$MEAN_COUNT_ALL_YEARS_PER_SID <- df$LUFTTEMP_AM_ERDB_MINIMUM_AGG_TOTAL_TRIG / df$COUNT_YEARS_PER_SID
      return(df)
    },
    triggerExpressionFilter = function(df){
      return(df$LUFTTEMP_AM_ERDB_MINIMUM_AGG_MONTH_TRIG > df$MEAN_COUNT_ALL_YEARS_PER_SID)
    }
  ),
  list(
    observationCols = c("NIEDERSCHLAGSHOEHE"),
    observationTimeFilter = "MONTH = 1 and NIEDERSCHLAGSHOEHE > 0",
    observationMap = function(df){
      sidColNameYearsUsed <- "yearSID"
      yearsUsed <- select(df, "STATIONS_ID", "YEAR")
      yearsUsed <- agg(groupBy(yearsUsed, yearsUsed$STATIONS_ID), OBS_COUNT_YEARS_PER_SID = SparkR::countDistinct(yearsUsed$YEAR))
      yearsUsed <- select(yearsUsed, SparkR::alias(yearsUsed$STATIONS_ID, sidColNameYearsUsed ), yearsUsed$OBS_COUNT_YEARS_PER_SID )
      df <- join(df, yearsUsed, df$STATIONS_ID == yearsUsed[[sidColNameYearsUsed]])
      df$OBS_MEAN_COUNT_NIEDERSCHLAGSHOEHE <- df$NIEDERSCHLAGSHOEHE_AGG_TOTAL_OBS / df$OBS_COUNT_YEARS_PER_SID 
      return(df)
    },
    observationExpression = function(df){
      return(df$NIEDERSCHLAGSHOEHE_AGG_MONTH_OBS > df$OBS_MEAN_COUNT_NIEDERSCHLAGSHOEHE)
    }
  ),
  joinWithNextYear,
  triggerAggOperation = SparkR::count,
  observationAggOperation = SparkR::count
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
  triggerAggOperation = SparkR::mean,
  observationCols = c("LUFTTEMPERATUR_MINIMUM"),
  observationTimeFilter = "MONTH = 1 or MONTH = 2",
  observationExpression = function(df){
    return(df$LUFTTEMPERATUR_MINIMUM_AGG_MONTH_OBS < df$LUFTTEMPERATUR_MINIMUM_AGG_TOTAL_OBS)
  },
  observationAggOperation = SparkR::mean,
  joinExpression = joinWithNextYear,
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
  triggerAggOperation = SparkR::mean,
  observationCols = c("LUFTTEMPERATUR_MAXIMUM"),
  observationTimeFilter = "MONTH = 1 or MONTH = 2",
  observationExpression = function(df){
    return(df$LUFTTEMPERATUR_MAXIMUM_AGG_MONTH_OBS > df$LUFTTEMPERATUR_MAXIMUM_AGG_TOTAL_OBS)
  },
  observationAggOperation = SparkR::mean,
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
  triggerAggOperation = SparkR::mean,
  observationCols = c("NIEDERSCHLAGSHOEHE"),
  observationTimeFilter = "",
  observationExpression = function(df){
    return(df$NIEDERSCHLAGSHOEHE_AGG_MONTH_OBS > df$NIEDERSCHLAGSHOEHE_AGG_TOTAL_OBS)
  },
  observationAggOperation = SparkR::mean,
  joinExpression = function(triggerTotalDf, observationTotalDf){ 
    return(triggerTotalDf$STATIONS_ID == observationTotalDf$SID & triggerTotalDf$YEAR == observationTotalDf$Y)
  },
  folderName = "feb_nass_jahr_nass"
)

clInputMartiniWinter <- list(
  pathToMeta = "/home/dhaeb/dvl/data/dwd/kl/KL_Tageswerte_Beschreibung_Stationen4.txt",
  pathToData = "/home/dhaeb/dvl/data/dwd/kl/kl_total.csv",
  schema = klSchema,
  triggerCols = c("SCHNEEHOEHE"),
  triggerTimeFilter = "MONTH = 11 and DAY BETWEEN 10 and 12",
  triggerExpressionFilter = function(df){
    return(df$SCHNEEHOEHE_AGG_MONTH_TRIG > 0)
  },
  triggerAggOperation = SparkR::mean,
  observationCols = c("LUFTTEMPERATUR_MINIMUM"),
  observationTimeFilter = "MONTH = 1 or MONTH = 2",
  observationExpression = function(df){
    return(df$LUFTTEMPERATUR_MINIMUM_AGG_MONTH_OBS < df$LUFTTEMPERATUR_MINIMUM_AGG_TOTAL_OBS)
  },
  observationAggOperation = SparkR::mean,
  joinExpression = function(triggerTotalDf, observationTotalDf){ 
    return(triggerTotalDf$STATIONS_ID == observationTotalDf$SID & triggerTotalDf$YEAR == observationTotalDf$Y)
  },
  folderName = "martini_winter"
)


aggregateByStatIdAndYear <- function(df, colsNames, aggColSuffix, aggFunction) {
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
    return(aggFunction(e))
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

aggregateByStatId <- function(df, colNames, joinable, aggColSuffix, aggFunction){
  for(colName in colNames){
    # calculate the aggregation over all years
    aggColName <- paste(colName, aggColSuffix, sep="")
    stationIdColName <- paste(colName, "_STATIONS_ID", sep="")
    dfColAgg <- agg(groupBy(df, df$STATIONS_ID), aggCol = aggFunction(df[[colName]]))
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
  triggerDfColAggMonth <- aggregateByStatIdAndYear(triggerDf, clInput$triggerCols, "_AGG_MONTH_TRIG", clInput$triggerAggOperation)
  #     trigger columns aggregation by month (STATIONS_ID)
  triggerDfColAggMonth <- aggregateByStatId(triggerDf, clInput$triggerCols, triggerDfColAggMonth, "_AGG_TOTAL_TRIG", clInput$triggerAggOperation)
  
  # map trigger df if needed
  if(!is.null(clInput$triggerMap)){
    triggerDfColAggMonth <- clInput$triggerMap(triggerDfColAggMonth)
  }
 
  #     just use data, which is related to the country lore
  triggerTotalDf <- SparkR::filter(triggerDfColAggMonth, clInput$triggerExpressionFilter(triggerDfColAggMonth))
  cache(triggerTotalDf)
  
  countFullfillingTriggeringRule <- count(triggerTotalDf)
  
#   print(SparkR::head(triggerTotalDf,100))
#   readline(prompt="Press [enter] to continue")


  #   observation columns filter
  if(nchar(clInput$observationTimeFilter) == 0){
    observationDf <- dataDf
  }  else {
    observationDf <- SparkR::filter(dataDf, clInput$observationTimeFilter )
  }
  #     columns aggregation by month (STATIONS_ID, YEAR)
  observationDfColAggMonth <- aggregateByStatIdAndYear(observationDf, clInput$observationCols, "_AGG_MONTH_OBS", clInput$observationAggOperation)
  
  #     observation columns aggregation by month (STATIONS_ID)
  observationDfColAggMonth <- aggregateByStatId(observationDf, clInput$observationCols, observationDfColAggMonth, "_AGG_TOTAL_OBS", clInput$observationAggOperation)
  
  # map observation df if needed
  if(!is.null(clInput$observationMap)){
    observationDfColAggMonth <- clInput$observationMap(observationDfColAggMonth)
  }
  

  
  # extract other columns using functional programming
  otherColumns <- Map(function(e){ 
      observationDfColAggMonth[[e]]
    }, Filter(function(e){
          e != "STATIONS_ID" & e != "YEAR"
       }, names(observationDfColAggMonth)))
  # rename STATION_ID and YEAR of observationDfColAggMonth
  observationTotalDf <- do.call("select", append(list(observationDfColAggMonth, SparkR::alias(observationDfColAggMonth$STATIONS_ID, "SID"), SparkR::alias(observationDfColAggMonth$YEAR, "Y")), otherColumns))
  cache(observationTotalDf)
#   print(SparkR::head(observationTotalDf,100))
#   readline(prompt="Press [enter] to continue")  
  
    
  # join into final data frame to count the results
  countablePreresult <- join(triggerTotalDf, observationTotalDf, clInput$joinExpression(triggerTotalDf, observationTotalDf))
  cache(countablePreresult)
  # count all
  countAllYearsPerStationFullfillingTrigger <- agg(groupBy(countablePreresult, countablePreresult$STATIONS_ID), countAll = count(countablePreresult$YEAR))
  # count fullfilling rule
  oberservationPreFilteredCountable <- SparkR::filter(countablePreresult, clInput$observationExpression(countablePreresult))
  countAllYearsPerStationFullfillingObservation <- agg(groupBy(oberservationPreFilteredCountable , oberservationPreFilteredCountable $SID), cFullfilling = count(oberservationPreFilteredCountable$Y))
  countAllYearsPerStationFullfillingObservation$cFullfillingQuad <- countAllYearsPerStationFullfillingObservation$cFullfilling * countAllYearsPerStationFullfillingObservation$cFullfilling
  
  # the final join
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
    percentageFullfillingRule <- sparkRSum(df, df$cFullfilling) / sparkRSum(df, df$countAll)
    return(percentageFullfillingRule)
  }
  folderPrefix <- "intermeditate_results/"
  
  
  addDataPair <- function(a,b){
    c(a, b)
  }
  filterForBundesland <- function(bl){
    addDataPair(bl,aggregateResults(totalResult$Bundesland == bl))
  }
  generalDataDf <- data.frame(
    addDataPair('total', aggregateResults()),
    addDataPair('N', aggregateResults(totalResult$Lage == "N")),
    addDataPair('O', aggregateResults(totalResult$Lage == "O")),
    addDataPair('S', aggregateResults(totalResult$Lage == "S")),
    addDataPair('W', aggregateResults(totalResult$Lage == "W")),
    filterForBundesland("Baden-Württemberg"),
    filterForBundesland("Bayern"),
    filterForBundesland("Berlin"),
    filterForBundesland("Brandenburg"),
    filterForBundesland("Bremen"),
    filterForBundesland("Hamburg"),
    filterForBundesland("Hessen"),
    filterForBundesland("Mecklenburg-Vorpommern"),
    filterForBundesland("Niedersachsen"),
    filterForBundesland("Nordrhein-Westfalen"),
    filterForBundesland("Rheinland-Pfalz"),
    filterForBundesland("Saarland"),
    filterForBundesland("Sachsen"),
    filterForBundesland("Sachsen-Anhalt"),
    filterForBundesland("Schleswig-Holstein"),
    filterForBundesland("Thüringen")
  )
  txtFileName <- paste(folderPrefix, clInput$folderName, ".txt", sep = "")
  print(head(generalDataDf))
  write.csv(x = t(generalDataDf), file = txtFileName, col.names= c("agg_area", "value"), row.names = FALSE) 
  readline("press!")
  exportCsv <- select(totalResult, totalResult$countAll, totalResult$cFullfilling, totalResult$rel, totalResult$longitude, totalResult$latitude, totalResult$Stationsname, totalResult$Bundesland, totalResult$Lage, totalResult$Statationshoehe, totalResult$von_datum, totalResult$bis_datum)
  exportCsv <- orderBy(exportCsv, exportCsv$rel)
  pathToResultSplits <- paste(folderPrefix, clInput$folderName, sep = "")
  write.df(exportCsv, pathToResultSplits, "com.databricks.spark.csv", "overwrite")
  file_list <- list.files(path = pathToResultSplits, pattern = "part-\\d{5}")
  outputCsvFile <- file(paste(pathToResultSplits, "/", clInput$folderName, ".csv", sep = ""), 'w')
  for(filename in file_list){
    pathToFile <- paste(pathToResultSplits, "/", filename, sep="")
    if(file.info(pathToFile)$size > 0){
      print(pathToFile)
      fhandle <- file(pathToFile, 'r')
      cat(readLines(fhandle), file = outputCsvFile, append = TRUE, sep = "\n")
    } 
  }
  
  return(totalResult)
}
