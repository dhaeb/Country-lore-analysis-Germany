leipzigDF <- read.csv(file = "C:/Users/Max/Desktop/leipzig-Rdf.csv")
minTemp <- aggregate(list(minTemp = leipzigDF$LUFTTEMPERATUR_MINIMUM),by = list(Year = leipzigDF$YEAR),FUN ="mean")
maxTemp <- aggregate(list(maxTemp = leipzigDF$LUFTTEMPERATUR_MAXIMUM),by = list(Year = leipzigDF$YEAR),FUN = "mean")
niedMean <- aggregate(list(rainMean = leipzigDF$NIEDERSCHLAGSHOEHE),by = list(Year = leipzigDF$YEAR),FUN = "sum")
head(niedMean)
tempDf <- merge(minTemp,maxTemp,by.x="Year", by.y = "Year")
tempDf$meanTemp <- rowMeans(subset(tempDf, select = c(minTemp, maxTemp)), na.rm = TRUE)
plot(tempDf$Year,tempDf$meanTemp,ann=FALSE,type = "p")
title(xlab="Year")
title(ylab="Annual average temperature")

abline(lm(tempDf$meanTemp ~ tempDf$Year))

plot(niedMean$Year,niedMean$rainMean,ann=FALSE,type = "p")
title(xlab="Year")
title(ylab="Annual precipitation")

abline(lm(niedMean$rainMean ~ niedMean$Year))
abline(lm(tempDf$meanTemp ~ tempDf$Year))
