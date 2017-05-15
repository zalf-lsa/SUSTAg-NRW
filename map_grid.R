
# Clear Environment Variables
rm(list = ls())

library(raster)

setwd("C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/grids")

##read land use file
r <- raster("agb_9110.asc") 
plot(r)

