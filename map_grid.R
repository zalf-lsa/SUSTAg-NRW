
# Clear Environment Variables
rm(list = ls())

library(raster)

setwd("C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/grids")

r <- raster("bkr_nrw_gk3.asc") 
plot(r)
title(main="bkr_nrw_gk3")

r <- raster("kreise_matrix.asc") 
plot(r)
title(main="kreise_matrix")

r <- raster("lu_resampled.asc") 
plot(r)
title(main="lu_resampled")

r <- raster("soil-profile-id_nrw_gk3.asc") 
plot(r)
title(main="soil-profile-id_nrw_gk3")

r <- raster("wheat_winter-wheat_agb_.asc") 
plot(r)
title(main="wheat_winter-wheat_agb_")

r <- raster("yearly_Nleach_.asc") 
plot(r)
title(main="yearly_Nleach_")

r <- raster("testXenia_elevation_.asc") 
plot(r)
title(main="testXenia_elevation_")
