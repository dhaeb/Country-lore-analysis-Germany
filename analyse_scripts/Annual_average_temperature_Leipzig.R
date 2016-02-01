leipzigDF <- read.csv(file = "C:/Users/Max/Desktop/leipzig-Rdf.csv")
minTemp <- aggregate(list(minTemp = leipzigDF$LUFTTEMPERATUR_MINIMUM),by = list(Year = leipzigDF$YEAR),FUN ="mean")
maxTemp <- aggregate(list(maxTemp = leipzigDF$LUFTTEMPERATUR_MAXIMUM),by = list(Year = leipzigDF$YEAR),FUN = "mean")
niedMean <- aggregate(list(rainSum = leipzigDF$NIEDERSCHLAGSHOEHE),by = list(Year = leipzigDF$YEAR, SID = leipzigDF$SID),FUN = "sum")
s2928 <- subset(niedMean,SID == 2928)
s2932 <- subset(niedMean, SID == 2932)

niedTotal <- merge(s2928,s2932,by.x="Year",by.y="Year", all = TRUE)
niedTotal$rainMean <- rowMeans(subset(niedTotal,select = c(rainSum.x,rainSum.y)), na.rm = TRUE)
head(niedTotal)
tempDf <- merge(minTemp,maxTemp,by.x="Year", by.y = "Year")
tempDf$meanTemp <- rowMeans(subset(tempDf, select = c(minTemp, maxTemp)), na.rm = TRUE)
plot(tempDf$Year,tempDf$meanTemp,ann=FALSE,type = "p")
title(xlab="Year")
title(ylab="Annual average temperature")

abline(lm(tempDf$meanTemp ~ tempDf$Year))

axis(side=1,c(1950:2015))
axis(side=2,c(7.0,12.0))

plot(niedTotal$Year,niedTotal$rainMean,ann=FALSE,type = "p")
title(xlab="Year")
title(ylab="Annual precipitation")

abline(lm(niedTotal$rainMean ~ niedTotal$Year))
