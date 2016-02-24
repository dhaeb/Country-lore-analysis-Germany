setwd("/Users/martinmac/Country-lore-analysis-Germany")
source("analyse_scripts/COR_FUNCTION.R")

# source("/Users/martinmac/Country-lore-analysis-Germany/COR_FUNCTION.R")

csvDfT <-
    read.csv(
        "Outputs/not_used_or_broken/tropfen_januar_schnee_mai.csv", header = FALSE, sep = ",",
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
head(dfT30[[1]],100)
# afterFilterCount: number of station
dfT30[[4]]
# [1] 428
st91 <- filter(dfT30[[1]], SID==91)
st91

################################
# correlation
################################
corSONNEt<- cal_corT(dfT30[[1]],dfT30[[2]])
tropfenO <- left_join(corSONNEt[[1]],stationDf_csv)
head(tropfenO)
# write.csv(tropfen,"Outputs/tropfen_januar_schnee_mai_COR.csv")

tropfenO$sid2 <- tropfenO$SID
nrow(tropfenO)
data.frame(tropfenO)
tropfen <- tropfenO[complete.cases(tropfenO$COR),]
nrow(tropfen)
tropfen <- select(tropfen, SID, yearCount,sid2,COR,longitude, latitude, Stationsname, Bundesland, Lage,Statationshoehe,von_datum,bis_datum)
write.table(tropfen,"Outputs/tropfen_januar_schnee_mai_COR.csv",col.names=FALSE)

nrow(tropfen)
head(data.frame(tropfen),10)

tDf <- data.frame(select(tropfen, COR))
deTDf <- data.frame(tDf %>% select(2) %>% summarise_each(funs(max,min,over4,over3)))
deTDf$Lage <-"DE"
deTDf <- select(deTDf,Lage,everything())
deTDf

subTDf<- dplyr::select(tropfen, Lage,COR) 
subTDf <- subTDf[complete.cases(tDf$COR),]
reDf <- data.frame(subTDf %>% group_by(Lage) %>% summarise_each(funs(max,min,over4,over3)))
reDf <- select(reDf, -contains("SID"))
colnames(reDf) <- c("Lage","max","min","over4","over3")
ncol(deTDf)
statTDf <- rbind(deTDf,reDf)
statTDf$num <- c(162,11,11,96,44)
statTDf

countL <- data.frame(subTDf %>% group_by(Lage) %>% summarise(num=count(Lage)))
countL

# get stations with at least 10 years with snow in may
schneeMai <- filter(dfT30[[1]],allMeanSCHNEEm>0)
schneeMai <- ddply(schneeMai,.(SID),transform, snowYearCount = length(YEAR))
nrow(schneeMai)
head(schneeMai)
schneeMaiF <- filter(schneeMai,snowYearCount>30)
head(schneeMaiF)
yearCountDf <- select(schneeMai,SID,yearCount)
yearCountDf <- unique(yearCountDf)
yearCountDf
corSM <- cal_corT(schneeMaiF,yearCountDf)
corSMS <- left_join(corSM[[1]],stationDf_csv)
corSMS 

write.csv(corSMS,"Outputs/tropfen_januar_schnee_mai_COR_30.csv")

corSMSx <-select(corSMS,SID,COR,yearCount,Statationshoehe,Stationsname,Bundesland,Lage) 
corSMSx
colnames(corSMSx) <- c("SID","COR","yearCount","Statationshoehe","Stationsname","Bundesland","Lage")
digit <-c(0,0,3,0,0,0,0,0)
corSMSx <- make_xtable(digit,corSMSx) 

st4933 <- filter(dfT30[[1]], SID==4933)
st4933

countSM <- data.frame(schneeMaiF %>% group_by(SID) %>% summarise(num=count(SID)))
nrow(countSM)
colnames(countSM) <- c("SID","yearCount")
