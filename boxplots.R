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

plot <- ggplot(dataset, aes(x = bkr, y = Nminfert)) + 
  geom_boxplot(aes(fill=factor(rotation)))
plot <- plot + facet_wrap(~ crop)
plot <- plot + theme_bw()
plot

