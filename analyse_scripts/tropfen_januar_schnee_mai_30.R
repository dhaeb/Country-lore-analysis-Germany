setwd("/Users/martinmac/Country-lore-analysis-Germany")
source("analyse_scripts/COR_FUNCTION.R")

# source("/Users/martinmac/Country-lore-analysis-Germany/COR_FUNCTION.R")

csvDfT <-
    read.csv(
        "Outputs/tropfen_januar_schnee_mai.csv", header = FALSE, sep = ",",
        col.names = c(
            "SID", "YEAR", "allMeanREGENj", "allMeanSCHNEEj","allMeanREGENm", "allMeanSCHNEEm"
        )
    )

nrow(csvDfT)
# [1] 30171

# select columns 
dfT <- select(csvDfT,-allMeanSCHNEEj,-allMeanREGENm)

# add yearCount
dfT <- ddply(dfT,.(SID),transform, yearCount = length(YEAR))
head(dfT)
# SID YEAR allMeanREGENj allMeanSCHNEEm yearCount
# 1   1 1937     1.3580645              0        48
# 2   1 1938     2.9806452              0        48
# 3   1 1939     1.5225806              0        48

# keep stations with more than 30 years records
dfT30 <- filterDf(dfT, 30)
# OUTPUT of filterDf() : (minFilterDf,dfYearCount for ALL stations, beforeFilterCount, afterFilterCount)
# after filter, number of records
nrow(dfT30[[1]])
# [1] 22539

# afterFilterCount: number of station
dfT30[[4]]
# [1] 428

################################
# correlation
################################
corSONNEt<- cal_corT(dfT30[[1]],dfT30[[2]])
tropfen <- left_join(corSONNEt[[1]],stationDf_csv)

head(tropfen)
# write.csv(tropfen,"Outputs/tropfen_januar_schnee_mai_COR.csv")

tropfen$sid2 <- tropfen$SID

tropfen <- select(tropfen, SID, yearCount,sid2,COR,longitude, latitude, Stationsname, Bundesland, Lage,Statationshoehe,von_datum,bis_datum)
head(tropfen)
write.table(tropfen,"Outputs/tropfen_januar_schnee_mai_COR.csv",col.names=FALSE)

