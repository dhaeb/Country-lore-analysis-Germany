# correlation: for each station, tag vs wochen through all years, for each weather measurement
###### correlation is not supported in sparkR < 1.6 --> use function

library(dplyr)
library(plyr)

# FILTER: keep only stations with years of recording >= minSample
filterDf <- function(df, minSample) {
    # before filter: how many stations
    dfYearCount <- df %>% group_by(SID) %>% select(SID, yearCount) %>% distinct()
    beforeFilterCount <- nrow(dfYearCount)
    minFilterDf <- filter(df, yearCount >= minSample)
    # after filter: how many stations
    dfYearCountFiltered <- filter(dfYearCount, yearCount >= minSample)
    afterFilterCount <- nrow(dfYearCountFiltered)
    return(list(minFilterDf,dfYearCount, beforeFilterCount, afterFilterCount))
}

# calculate correlation for each station (SIEBENSCHLAEFTERTAG)
cal_cor <- function(df,count_df,weather) {
    if (weather == "S") {
        corDf <- df %>% group_by(SID) %>% dplyr::summarise(COR = cor(allMeanSONNE,allMeanSONNEw))
    } else if (weather == "R") {
        corDf <- df %>% group_by(SID) %>% dplyr::summarise(COR = cor(allMeanREGEN,allMeanREGENw))
    } else if (weather == "T") {
        corDf <- df %>% group_by(SID) %>% dplyr::summarise(COR = cor(allMeanTEMP,allMeanTEMPw))
    }
    # join year count and correlation
    joinCor <- inner_join(count_df,corDf,by = "SID")
    joinCorDES <- joinCor %>% arrange(-COR)
    joinCorASC <- joinCor %>% arrange(COR)
    return(list(joinCorDES,joinCorASC))
}

# calculate correlation for each station using PERIOD 2 (SIEBENSCHLAEFTERTAG)
cal_cor2 <- function(df,count_df,weather) {
    if (weather == "S") {
        corDf <- df %>% group_by(SID) %>% dplyr::summarise(COR = cor(allMeanSONNE2,allMeanSONNEw))
    } else if (weather == "R") {
        corDf <- df %>% group_by(SID) %>% dplyr::summarise(COR = cor(allMeanREGEN2,allMeanREGENw))
    } else if (weather == "T") {
        corDf <- df %>% group_by(SID) %>% dplyr::summarise(COR = cor(allMeanTEMP2,allMeanTEMPw))
    }
    # join year count and correlation
    joinCor <- inner_join(count_df,corDf,by = "SID")
    joinCorDES <- joinCor %>% arrange(-COR)
    joinCorASC <- joinCor %>% arrange(COR)
    return(list(joinCorDES,joinCorASC))
}

# calculate correlation for each station (TROPFEN)
cal_corT <- function(df,count_df) {
    corDf <- df %>% group_by(SID) %>% dplyr::summarise(COR = cor(allMeanREGENj,allMeanSCHNEEm))
    # join year count and correlation
    joinCor <- inner_join(count_df,corDf,by = "SID")
    joinCorDES <- joinCor %>% arrange(-COR)
    joinCorASC <- joinCor %>% arrange(COR)
    return(list(joinCorDES,joinCorASC))
}

