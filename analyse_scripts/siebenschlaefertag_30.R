source("/Users/martinmac/Country-lore-analysis-Germany/COR_FUNCTION.R")

# read csv
setwd("/Users/martinmac/Big_Data_Prak_Lani")

csvDf <-
    read.csv(
        "Outputs/siebenschlaefertag_TWO.csv", header = FALSE, sep = ",",
        col.names = c(
            "SID", "YEAR", "allMeanSONNE", "allMeanREGEN", "allMeanTEMP","allMeanSONNE2", "allMeanREGEN2", "allMeanTEMP2", "allMeanSONNEw", "allMeanREGENw","allMeanTEMPw"
        )
    )
# nrow(csvDf)
# [1] 16694

### not enough finite observations --> remove SID with less than 3 YEARS ###
# add yearCount
df <- ddply(csvDf,.(SID),transform, yearCount = length(YEAR))
head(df)
# SID YEAR allMeanSONNE allMeanREGEN allMeanTEMP allMeanSONNE2 allMeanREGEN2 allMeanTEMP2
# 1   3 1935     9.033333     1.666667    21.86667      8.600000      1.053846     19.33077
# 2   3 1936     4.700000     4.266667    16.90000      4.761538      7.476923     17.95385
# 3   3 1937     9.733333     0.000000    17.93333      6.876923      1.669231     17.43846
# allMeanSONNEw allMeanREGENw allMeanTEMPw yearCount
# 1         8.734         0.942       18.062        75
# 2         4.974         3.608       16.792        75
# 3         5.826         1.348       17.566        75

# BEFORE setting sample size threshold
nrow(df)
# [1] 16694

# keep stations with more than 30 years records
df30 <- filterDf(df, 30)
# OUTPUT of filterDf() : (minFilterDf,dfYearCount, beforeFilterCount, afterFilterCount)
head(df30[[1]])
# SID YEAR allMeanSONNE allMeanREGEN allMeanTEMP allMeanSONNE2 allMeanREGEN2
# 1   3 1935     9.033333     1.666667    21.86667      8.600000      1.053846
# 2   3 1936     4.700000     4.266667    16.90000      4.761538      7.476923
# 3   3 1937     9.733333     0.000000    17.93333      6.876923      1.669231
# allMeanTEMP2 allMeanSONNEw allMeanREGENw allMeanTEMPw yearCount
# 1     19.33077         8.734         0.942       18.062        75
# 2     17.95385         4.974         3.608       16.792        75
# 3     17.43846         5.826         1.348       17.566        75

# before filter, number of records (from 'siebenschlaefertag_COR.R')
# [1] 16694

# after filter, number of records
nrow(df30[[1]])
# [1] 12095

head(df30[[2]]) # for ALL stations
# SID yearCount
# (int)     (int)
# 1     3        75
# 2    44        41
# 3    52        29

# beforeFilterCount: number of station = nrow(df30[[2]])
df30[[3]]
# [1] 587

# afterFilterCount: number of station
df30[[4]]
# [1] 231

################################
# correlation for SONNE
################################
corSONNE_with0 <- cal_cor(df30[[1]],df30[[2]],weather="S")
head(corSONNE_with0[[1]])
# SID yearCount       COR
# (int)     (int)     (dbl)
# 1  3666        33 0.9411375
# 2   377        39 0.7204946
# 3   554        38 0.5987215

## OBSERVATION: some records are recorded as 0.0 shall be actually NO RECORD
# SID 3666 : extremely high
df3666 <- filter(df,SID == 3666) # 13 of total 23 years allMeanSONNE=0 --> SID=3666 REMOVE from analysis
df377 <- filter(df,SID == 377) # 6 of 39 year allMeanSONNE=0 --> remove records with allMeanSONNE=0
df554 <- filter(df,SID == 553) # OK

# TODO: if 15% of the years with allMeanSONNE=0, remove station

# remove 3666
df30_no3666 <- filter(df30[[1]],SID != 3666)
nrow(df30_no3666)
# [1] 12062

# ==> remove all records with allMeanSONNE=0 for SONNE
df30_no0 <- filter(df30_no3666,allMeanSONNE != 0)
nrow(df30_no0)
# [1] 12042

corSONNE<- cal_cor(df30_no0,df30[[2]],weather="S")
head(corSONNE[[1]])
# SID yearCount       COR
# (int)     (int)     (dbl)
# 1   554        38 0.5987215
# 2  2474        32 0.5192512
# 3   186        33 0.5189173

head(corSONNE[[2]])
# SID yearCount         COR
# (int)     (int)       (dbl)
# 1  2812        30 -0.07001763
# 2  1957        37 -0.05270790
# 3  1694        36 -0.05166401

corSONNE2 <- cal_cor2(df30_no0,df30[[2]],weather="S")
head(corSONNE2[[1]])
# SID yearCount       COR
# (int)     (int)     (dbl)
# 1  3096        54 0.7192655
# 2  1219        31 0.6869555
# 3  3612        41 0.6836951

head(corSONNE2[[2]])
# SID yearCount       COR
# (int)     (int)     (dbl)
# 1  3379        30 0.1709289
# 2  2812        30 0.2188154
# 3  2712        42 0.2298808

################################
# correlation for REGEN
################################
corREGEN <- cal_cor(df30_no0,df30[[2]],weather="R")
head(corREGEN[[1]])
# SID yearCount       COR
# (int)     (int)     (dbl)
# 1  1833        36 0.4417299
# 2  5099        39 0.4353502
# 3  4927        34 0.4264924
head(corREGEN[[2]])
# SID yearCount        COR
# (int)     (int)      (dbl)
# 1  4928        37 -0.3194272
# 2  2812        30 -0.2888401
# 3  4411        32 -0.2779664

corREGEN2 <- cal_cor2(df30_no0,df30[[2]],weather="R")
head(corREGEN2[[1]])
# SID yearCount       COR
# 1  1833        36 0.6759977
# 2  4911        46 0.6181167
# 3  1440        33 0.5889522
head(corREGEN2[[2]])
# SID yearCount        COR
# (int)     (int)      (dbl)
# 1  1757        37 0.05846233
# 2  3811        32 0.06868939
# 3  3376        51 0.09846005

################################
# correlation for TEMP
################################
corTEMP<- cal_cor(df30_no0,df30[[2]],weather="T")
head(corTEMP[[1]])
# SID yearCount       COR
# (int)     (int)     (dbl)
# 1  2474        32 0.5938428
# 2   554        38 0.5909633
# 3  2456        64 0.5729734
head(corTEMP[[2]])
# SID yearCount          COR
# (int)     (int)        (dbl)
# 1  2812        30 -0.004937565
# 2  5890        40  0.112450709
# 3   851        37  0.146727817

corTEMP2<- cal_cor2(df30_no0,df30[[2]],weather="T")
head(corTEMP2[[1]])
# SID yearCount       COR
# (int)     (int)     (dbl)
# 1  4642        38 0.7033304
# 2  5078        53 0.6828382
# 3   591        50 0.6819280

head(corTEMP2[[2]])
# SID yearCount       COR
# (int)     (int)     (dbl)
# 1  3379        30 0.3908252
# 2   259        31 0.3947151
# 3  3671        30 0.4047849

### join all results for sample size > 30
corSONNE30 <- inner_join(corSONNE[[1]],corSONNE2[[1]], by="SID")
corSONNE30 <- select(corSONNE30,-yearCount.y)
colnames(corSONNE30) <- c("SID","yearCount","corSONNE30z1","corSONNE30z2")
head(corSONNE30)

corREGEN30 <- inner_join(corREGEN[[1]],corREGEN2[[1]], by="SID")
corREGEN30 <- select(corREGEN30,-yearCount.x, -yearCount.y)
colnames(corREGEN30) <- c("SID","corREGEN30z1","corREGEN30z2")
head(corREGEN30)

corTEMP30 <- inner_join(corTEMP[[1]],corTEMP2[[1]], by="SID")
corTEMP30 <- select(corTEMP30,-yearCount.x, -yearCount.y)
colnames(corTEMP30) <- c("SID","corTEMP30z1","corTEMP30z2")
head(corTEMP30)

corS30 <- inner_join(corSONNE30,corREGEN30)
corS30 <- inner_join(corS30 ,corTEMP30)
head(corS30)
# SID yearCount corSONNE30z1 corSONNE30z2 corREGEN30z1 corREGEN30z2 corTEMP30z1 corTEMP30z2
# (int)     (int)        (dbl)        (dbl)        (dbl)        (dbl)       (dbl)       (dbl)
# 1   554        38    0.5987215    0.6586967   0.01733031    0.3003030   0.5909633   0.5961822
# 2  2474        32    0.5192512    0.6594000   0.24990728    0.4899366   0.5938428   0.6475390
# 3   186        33    0.5189173    0.6783404   0.32365650    0.5607874   0.4446224   0.6025402
# 4  3096        54    0.5171472    0.7192655   0.17604163    0.3900902   0.5639885   0.6705793
# 5  3101        41    0.4725299    0.6731341   0.23895728    0.3779699   0.5073963   0.6580538
# 6 14311        34    0.4705776    0.6392158   0.12730824    0.3487567   0.5002761   0.5854875

corS30s2 <- corS30 %>% arrange(-corSONNE30z2)
head(corS30s2)
# SID yearCount corSONNE30z1 corSONNE30z2 corREGEN30z1 corREGEN30z2 corTEMP30z1 corTEMP30z2
# (int)     (int)        (dbl)        (dbl)        (dbl)        (dbl)       (dbl)       (dbl)
# 1  3096        54    0.5171472    0.7192655   0.17604163    0.3900902   0.5639885   0.6705793
# 2  1219        31    0.4642277    0.6869555   0.09448495    0.4696991   0.5688375   0.6371283
# 3  3612        41    0.2293451    0.6836951   0.04892274    0.1641728   0.4666412   0.6072257
# 4   186        33    0.5189173    0.6783404   0.32365650    0.5607874   0.4446224   0.6025402
# 5  3790        35    0.3834025    0.6773688   0.10781995    0.3263129   0.4839642   0.6351453
# 6  1079        53    0.4292961    0.6739233   0.07244551    0.3574683   0.5086465   0.6558442

### JOIN with locataion df for visulization
stationDf_csv <-
    read.csv(
        "Outputs/stationDf.csv", header = FALSE, sep = ",",
        col.names = c(
            "STATIONS_ID", "von_datum", "bis_datum", "Statationshoehe", "longitude", "latitude", "Stationsname","Bundesland", "Lage"
        )
    )
colnames(stationDf_csv)[1] <- "SID"
colnames(corS30s2)[3:8] <-c("corSONNEz1", "corSONNEz2", "corREGENz1", "corREGENz2", "corTEMPz1", "corTEMPz2")

sieben <- left_join(corS30s2,stationDf_csv)
head(sieben)

write.csv(sieben,"Outputs/siebenschlaefertag_COR.csv")
