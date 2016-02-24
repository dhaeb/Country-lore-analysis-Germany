# calculate corelations and export files for visualization
library(dplyr)
library(plyr)

setwd("/Users/martinmac/Country-lore-analysis-Germany/")
sieben <- read.csv("Outputs/not_used_or_broken/sieben2z.csv")

df <- ddply(sieben,.(SID),transform, yearCount = length(YEAR))

# keep stations with more than 30 years records
df30 <- filterDf(df, 30)
head(df30[[1]])
nrow(df30[[1]])
################################
# correlation for SONNE
################################
corSONNE_with0 <- cal_cor(df30[[1]],df30[[2]],weather="S")
head(corSONNE_with0[[1]])
## OBSERVATION: some records are recorded as 0.0 shall be actually NO RECORD
# SID 3666 : extremely high
df3666 <- filter(df,SID == 3666) # 13 of total 23 years allMeanSONNE=0 --> SID=3666 REMOVE from analysis
df377 <- filter(df,SID == 377) # 6 of 39 year allMeanSONNE=0 --> remove records with allMeanSONNE=0
df554 <- filter(df,SID == 553) # OK
# remove 3666
df30_no3666 <- filter(df30[[1]],SID != 3666)
nrow(df30_no3666)
# ==> remove all records with allMeanSONNE=0 for SONNE
df30_no0 <- filter(df30_no3666,allMeanSONNE != 0)
nrow(df30_no0)
#[1] 11965
corSONNE<- cal_cor(df30_no0,df30[[2]],weather="S")
head(corSONNE[[1]])
head(corSONNE[[2]])
corSONNE2 <- cal_cor2(df30_no0,df30[[2]],weather="S")
################################
# correlation for REGEN
################################
corREGEN <- cal_cor(df30_no0,df30[[2]],weather="R")
corREGEN2 <- cal_cor2(df30_no0,df30[[2]],weather="R")
################################
# correlation for TEMP
################################
corTEMP<- cal_cor(df30_no0,df30[[2]],weather="T")
corTEMP2<- cal_cor2(df30_no0,df30[[2]],weather="T")

### join all results for sample size > 30
corSONNE30 <- inner_join(corSONNE[[1]],corSONNE2[[1]], by="SID")
corSONNE30 <- select(corSONNE30,-yearCount.y)
colnames(corSONNE30) <- c("SID","yearCount","corSONNE30z1","corSONNE30z2")

corREGEN30 <- inner_join(corREGEN[[1]],corREGEN2[[1]], by="SID")
corREGEN30 <- select(corREGEN30,-yearCount.x, -yearCount.y)
colnames(corREGEN30) <- c("SID","corREGEN30z1","corREGEN30z2")

corTEMP30 <- inner_join(corTEMP[[1]],corTEMP2[[1]], by="SID")
corTEMP30 <- select(corTEMP30,-yearCount.x, -yearCount.y)
colnames(corTEMP30) <- c("SID","corTEMP30z1","corTEMP30z2")

corS30 <- inner_join(corSONNE30,corREGEN30)
corS30 <- inner_join(corS30 ,corTEMP30)
corS30s2 <- corS30 %>% arrange(-corSONNE30z2)

stationDf_csv <-
    read.csv(
        "Outputs/not_used_or_broken/stationDf.csv", header = FALSE, sep = ",",
        col.names = c(
            "STATIONS_ID", "von_datum", "bis_datum", "Statationshoehe", "longitude", "latitude", "Stationsname","Bundesland", "Lage"
        )
    )
colnames(stationDf_csv)[1] <- "SID"
colnames(corS30s2)[3:8] <-c("corSONNEz1", "corSONNEz2", "corREGENz1", "corREGENz2", "corTEMPz1", "corTEMPz2")

sieben1 <- left_join(corS30s2,stationDf_csv)
sieben1$sid2 <- sieben1$SID

sieben_s1 <- select(sieben1, SID, yearCount,sid2,corSONNEz1,longitude, latitude, Stationsname, Bundesland, Lage,Statationshoehe,von_datum,bis_datum)
sieben_s2 <- select(sieben1, SID, yearCount,sid2,corSONNEz2,longitude, latitude, Stationsname, Bundesland, Lage,Statationshoehe,von_datum,bis_datum)
sieben_r1 <- select(sieben1, SID, yearCount,sid2,corREGENz1,longitude, latitude, Stationsname, Bundesland, Lage,Statationshoehe,von_datum,bis_datum)
sieben_r2 <- select(sieben1, SID, yearCount,sid2,corREGENz2,longitude, latitude, Stationsname, Bundesland, Lage,Statationshoehe,von_datum,bis_datum)
sieben_t1 <- select(sieben1, SID, yearCount,sid2,corTEMPz1,longitude, latitude, Stationsname, Bundesland, Lage,Statationshoehe,von_datum,bis_datum)
sieben_t2 <- select(sieben1, SID, yearCount,sid2,corTEMPz2,longitude, latitude, Stationsname, Bundesland, Lage,Statationshoehe,von_datum,bis_datum)

write.table(sieben_s1,"Outputs/siebenschlaefertag_COR_s1.csv",col.names=FALSE)
write.table(sieben_s2,"Outputs/siebenschlaefertag_COR_s2.csv",col.names=FALSE)
write.table(sieben_r1,"Outputs/siebenschlaefertag_COR_r1.csv",col.names=FALSE)
write.table(sieben_r2,"Outputs/siebenschlaefertag_COR_r2.csv",col.names=FALSE)
write.table(sieben_t1,"Outputs/siebenschlaefertag_COR_t1.csv",col.names=FALSE)
write.table(sieben_t2,"Outputs/siebenschlaefertag_COR_t2.csv",col.names=FALSE)

# plots
sieben_t1 <- sieben_t1 %>% arrange(-corTEMPz1)
sieben_t1 <- data.frame(sieben_t1)
head(sieben_t1)
# SID yearCount sid2  corTEMPz1 longitude latitude                     Stationsname          Bundesland Lage
# 1 2474        32 2474 0.59384283   50.9104   6.4093          Jülich (Forsch.-Anlage) Nordrhein-Westfalen    W
# 2  554        38  554 0.59096328   51.8293   6.5365     Bocholt-Liedern (Wasserwerk) Nordrhein-Westfalen    W
# 3 2456        63 2456 0.57275739   53.5322   7.8806                            Jever       Niedersachsen    N
# 4  619        57  619 0.56939310   53.5788   6.6703               Borkum-Süderstraße       Niedersachsen    N
# 5 1219        30 1219 0.56884044   53.3449   7.1909                 Emden-Nesserland       Niedersachsen    N

tempRaw <- dplyr::select(df30_no0,SID,YEAR,yearCount,allMeanTEMP,allMeanTEMPw)
t2474 <- filter(tempRaw,SID==2474)

png(file = "Plots/T1_Juelich.png")
plot(t2474$allMeanTEMP,t2474$allMeanTEMPw, main = "Station: Jülich, Korrelation: 0.594 ",
     xlab="Durchschnittliche Temperatur im Siebenschläfertag (26.06 - 28.06)", ylab="Durchschnittliche Temperatur in den 7 Wochen danach (29.06 - 17.08)", pch=19, col = "darkgreen",
     cex.lab=1.2,cex.axis=1.2)
dev.off()

t554 <- filter(tempRaw,SID==554)
png(file = "Plots/T2_Bocholt-Liedern.png")
plot(t554$allMeanTEMP,t554$allMeanTEMPw, main = "Station: Bocholt-Liedern, Korrelation: 0.591 ",
     xlab="Durchschnittliche Temperatur im Siebenschläfertag (26.06 - 28.06)", ylab="Durchschnittliche Temperatur in den 7 Wochen danach (29.06 - 17.08)", pch=19, col = "darkgreen",
     cex.lab=1.2,cex.axis=1.2)
dev.off()

t2456 <- filter(tempRaw,SID==2456)
png(file = "Plots/T3_Jever.png")
plot(t2456$allMeanTEMP,t2456$allMeanTEMPw, main = "Station: Jever, Korrelation: 0.573 ",
     xlab="Durchschnittliche Temperatur im Siebenschläfertag (26.06 - 28.06)", ylab="Durchschnittliche Temperatur in den 7 Wochen danach (29.06 - 17.08)", pch=19, col = "darkgreen",
     cex.lab=1.2,cex.axis=1.2)
dev.off()