#https://www.r-bloggers.com/reading-multiple-files/

rm(list = ls())
library(datasets)
library(ggplot2)

setwd("C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/splitted-out")

file_list <- list.files(pattern="*_crop.csv")

for (file in file_list){
  
  # if the merged dataset doesn't exist, create it
  if (!exists("dataset")){
    dataset <- read.table(file, header=TRUE, sep=",")
  }
  
  # if the merged dataset does exist, append to it
  if (exists("dataset")){
    temp_dataset <-read.table(file, header=TRUE, sep=",")
    dataset<-rbind(dataset, temp_dataset)
    rm(temp_dataset)
  }
  
}

dataset$bkr <- factor(dataset$bkr)

plot <- ggplot(dataset, aes(x = bkr, y = RelDev)) + 
  geom_boxplot(aes(fill=factor(rotation)))
plot <- plot + facet_wrap(~ crop)
plot <- plot + theme_bw()
plot

#barley_spring-barley, barley_winter-barley, maize_silage-maize, mustard_,
#rape_winter-rape, triticale_winter-triticale, wheat_winter-wheat, potato_moderately-early-potato,
#sugar-beet, maize_grain-maize
cp_dataset <- subset(dataset, crop == "maize_grain-maize")
plot <- ggplot(cp_dataset, aes(x = bkr, y = hi)) + 
  geom_boxplot(aes(fill=factor(rotation)))
plot <- plot + theme_bw()
plot

#Kuopio meeting
rm(list = ls())
library(datasets)
library(ggplot2)

setwd("C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/splitted-out/")
#setwd("C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/out-kuopio/splitted-out/out-kuopio-2030-Nmin-33removal/")

#list files in the nested directories
mylist <- list.files(pattern="*_year.csv", recursive=TRUE)

#temp_dataset$resremoval <- rep(33,nrow(temp_dataset)) # make new column 

for (file in mylist){
  
  # if the merged dataset doesn't exist, create it
  if (!exists("dataset")){
    dataset <- read.table(file, header=TRUE, sep=",")
  }
  
  # if the merged dataset does exist, append to it
  if (exists("dataset")){
    temp_dataset <-read.table(file, header=TRUE, sep=",")
    dataset<-rbind(dataset, temp_dataset)
    rm(temp_dataset)
  }
  
}

levels(dataset$crop)[levels(dataset$crop)=="barley_spring-barley"] <- "SB"
levels(dataset$crop)[levels(dataset$crop)=="barley_winter-barley"] <- "WB"
levels(dataset$crop)[levels(dataset$crop)=="maize_silage-maize"] <- "SM"
levels(dataset$crop)[levels(dataset$crop)=="mustard_"] <- "CC"
levels(dataset$crop)[levels(dataset$crop)=="rape_winter-rape"] <- "WRa"
levels(dataset$crop)[levels(dataset$crop)=="triticale_winter-triticale"] <- "WTr"
levels(dataset$crop)[levels(dataset$crop)=="wheat_winter-wheat"] <- "WW"
levels(dataset$crop)[levels(dataset$crop)=="potato_moderately-early-potato"] <- "PO"
levels(dataset$crop)[levels(dataset$crop)=="sugar-beet_"] <- "SBee"
levels(dataset$crop)[levels(dataset$crop)=="maize_grain-maize"] <- "GM"

dataset$bkr <- factor(dataset$bkr)

plot <- ggplot(dataset, aes(x = crop, y = yield)) + geom_boxplot() + theme_bw()
plot + ggtitle("Pot-noheat-nofrost")


#dataset$yieldRounded<-round(dataset$yield, digits = -1)
#plot_viol <- ggplot(dataset, aes(x = crop, y = yieldRounded)) + geom_violin() + theme_bw()
#plot_viol

#yield, Nleach, Nminfert
plot <- ggplot(dataset, aes(x = bkr, y = yield)) + 
  #geom_boxplot(aes(fill=factor(RemovalRate)))
  geom_boxplot(aes(fill=factor(rotation)))
plot <- plot + facet_wrap(~ crop)
plot <- plot + theme_bw()
plot

dataset$rotation <- factor(dataset$rotation)
levels(dataset$rotation)

levels(dataset$rotation)[levels(dataset$rotation)=="1110"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="1120"] <- "WW-WB-WTr-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="1130"] <- "GM-WTr"
levels(dataset$rotation)[levels(dataset$rotation)=="2110"] <- "WB-SM-WTr"
levels(dataset$rotation)[levels(dataset$rotation)=="2120"] <- "WW-WB-GM"
levels(dataset$rotation)[levels(dataset$rotation)=="2130"] <- "SM-GM"
levels(dataset$rotation)[levels(dataset$rotation)=="3110"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="3120"] <- "WW-WB-WTr-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="3130"] <- "WW-GM"
levels(dataset$rotation)[levels(dataset$rotation)=="4110"] <- "WW-SM-WB-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="4120"] <- "WW-WB-WTr-GM"
levels(dataset$rotation)[levels(dataset$rotation)=="5110"] <- "WW-WB-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="5120"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="5130"] <- "WW-WTr-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="6110"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="6120"] <- "WW-WRa-GM"
levels(dataset$rotation)[levels(dataset$rotation)=="6130"] <- "WW-SBee-WW-PO"
levels(dataset$rotation)[levels(dataset$rotation)=="7110"] <- "WW-WB-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="7120"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="7130"] <- "WW-SB-WTr-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="8110"] <- "WW-WB-SBee"
levels(dataset$rotation)[levels(dataset$rotation)=="8120"] <- "WW-SM-WW-PO"
levels(dataset$rotation)[levels(dataset$rotation)=="8130"] <- "WW-WW-SBee"
levels(dataset$rotation)[levels(dataset$rotation)=="9110"] <- "WW-SB-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="9120"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="9130"] <- "WW-WB-WTr"

#bkr191_dataset <- subset(dataset, bkr == "191")

#yield, Nleach, Nminfert
plot <- ggplot(bkr191_dataset, aes(x = rotation, y = deltaOC)) + 
  geom_boxplot(aes(fill=factor(RemovalRate)))
plot <- plot + theme_bw()
plot

#test humus balance effectivity
dataset$KA5class <- factor(dataset$KA5class)
dataset$soiltype <- factor(dataset$soiltype)
plot <- ggplot(dataset, aes(x = soiltype, y = deltaOC)) + 
  geom_boxplot()
plot <- plot + facet_wrap(~ rotation)
plot <- plot + theme_bw()
plot <- plot + geom_hline(yintercept = 0)
plot

dataset$bkr <- factor(dataset$bkr)
plot <- ggplot(dataset, aes(x = bkr, y = deltaOC)) + 
  geom_boxplot(aes(fill=factor(rotation)))
plot <- plot + theme_bw()
#plot <- plot + ylim(NA, 200)
plot


#test to define OrgN classes
orgNdata = c(42,
             42,
             47,
             49,
             50,
             51,
             51,
             52,
             58,
             62,
             65,
             67,
             70,
             74,
             76,
             80,
             81,
             83,
             85,
             86,
             88,
             88,
             90,
             91,
             91,
             92,
             92,
             93,
             96,
             96,
             96,
             99,
             99,
             101,
             101,
             102,
             102,
             103,
             103,
             106,
             107,
             111,
             115,
             116,
             116,
             117,
             121,
             126,
             129,
             132,
             134,
             135,
             137,
             137,
             140,
             148,
             155,
             191,
             193
)

hist(orgNdata, breaks=3, col="red")

x <- orgNdata 
h<-hist(x, breaks=10, col="red",  
        main="Histogram with Normal Curve") 
xfit<-seq(min(x),max(x),length=40) 
yfit<-dnorm(xfit,mean=mean(x),sd=sd(x)) 
yfit <- yfit*diff(h$mids[1:2])*length(x) 
lines(xfit, yfit, col="blue", lwd=2)
