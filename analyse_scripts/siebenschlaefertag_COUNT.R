setwd("/Users/martinmac/Country-lore-analysis-Germany/")

source("analyse_scripts/siebenschlaefertag_COR.R")

options(digits = 8)
head(data.frame(sieben1))
# for Deutschland
countDf <- data.frame(select(sieben1, corSONNEz1, corSONNEz2,corREGENz1,corREGENz2,corTEMPz1,corTEMPz2))
deDf <- data.frame(countDf %>% select(2:7) %>% summarise_each(funs(max,min,over5,over4)))
deDf$Lage <-"DE"
deDf <- select(deDf,Lage,everything())
deDf

# stats in each Lage (N,W,E,S)
subCountDf <- dplyr::select(sieben1, Lage, corSONNEz1, corSONNEz2,corREGENz1,corREGENz2,corTEMPz1,corTEMPz2 )
reDf <- data.frame(subCountDf %>% select(2:8) %>% group_by(Lage) %>% summarise_each(funs(max,min,over5,over4)))
reDf <- select(reDf, -contains("SID"))
reDf
statDf <- rbind(deDf,reDf)
# determine column class
sapply(statDf, class)
# function to round all numeric columns
round_df <- function(df, digits) {
    nums <- vapply(df, is.numeric, FUN.VALUE = logical(1))
    df[,nums] <- round(df[,nums], digits = digits)
    (df)
}
statDf <- round_df(statDf, digits=3)

# Zeitraum 1
z1 <- select(statDf,contains("z1"))
z1g <- select(z1,contains("SONNE"),contains("REGEN"),contains("TEMP"))
colnames(z1g) <- c("Smax","Smin","S>0.5","S>0.4","Rmax","Rmin","R>0.5","R>0.4","Tmax","Tmin","T>0.5","T>0.4")
rownames(z1g) <-c("De","N","O","S","W")
write.table(z1g,"Outputs/siebenschlaefertag_z1.csv")

# Zeitraum 2
z2 <- select(statDf,contains("z2"))
z2g <- select(z2,contains("SONNE"),contains("REGEN"),contains("TEMP"))
colnames(z2g) <- c("Smax","Smin","S>0.5","S>0.4","Rmax","Rmin","R>0.5","R>0.4","Tmax","Tmin","T>0.5","T>0.4")
rownames(z2g) <-c("De","N","O","S","W")
write.table(z2g,"Outputs/siebenschlaefertag_z2.csv")
z1g

z1gt <- z1g
z1gt$ng <-c(228,48,47,79,54) 
z1gt$ngp <- (z1gt$`T>0.4`/z1gt$ng) *100
z1gt
colnames(z1gt) <- c("max","min",">0.5",">0.4","max","min",">0.5",">0.4","max","min",">0.5",">0.4","# Region","%")
digit <-c(0,3,3,0,0,3,3,0,0,3,3,0,0,0,1)
z1gx <- make_xtable(digit,z1gt) 

z2gt <- z2g
colnames(z2gt) <- c("max","min",">0.5",">0.4","max","min",">0.5",">0.4","max","min",">0.5",">0.4")
z2gx <- make_xtable(digit,z2gt) 

nrow(sieben1)
countSub <- data.frame(subCountDf %>% group_by(Lage) %>% summarise(num = count(Lage)))
colnames(countSub) <- c("Lage","Count")
countSub
head(subCountDf)

st5792 <- filter(dfT30[[1]], SID==5792)
st5792
