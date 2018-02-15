
#https://stackoverflow.com/questions/9436947/legend-properties-when-legend-only-t-raster-package
#http://neondataskills.org/R/Plot-Rasters-In-R/


# Clear Environment Variables
rm(list = ls())

library(raster)
library(rgdal)

setwd("C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/grids")

#
r <- raster("bkr_nrw_gk3.asc") 
plot(r)
title(main="bkr_nrw_gk3")

r <- raster("kreise_matrix.asc") 
plot(r)
title(main="kreise_matrix")

r <- raster("Kreise_N_amount_.asc") 
plot(r)
title(main="Kreise_N_amount_")

r <- raster("lu_resampled.asc") 
plot(r)
title(main="lu_resampled")

r <- raster("soil-profile-id_nrw_gk3.asc") 
plot(r)
title(main="soil-profile-id_nrw_gk3")

#############
#yearly data#
#############
r <- raster("yearly_avg_Nleach_.asc") 
plot(r)
title(main="avg N leaching (kg yr-1)")

r <- raster("yearly_stdev_Nleach_.asc") 
plot(r)
title(main="st dev N leaching (kg yr-1)")

##########

r <- raster("yearly_avg_waterperc_.asc") 
plot(r)
title(main="avg water perc (yr-1)")

r <- raster("yearly_stdev_waterperc_.asc") 
plot(r)
title(main="st dev water perc (yr-1)")

#########

r <- raster("yearly_avg_deltaOC_.asc") 
plot(r)
title(main="avg delta OC (% yr-1)")

r <- raster("yearly_stdev_deltaOC_.asc") 
plot(r)
title(main="st dev delta OC (yr-1)")

###########
#crop data#
###########
r <- raster("wheat_winter-wheat_avg_yield_.asc") 
plot(r)
title(main="avg WW yield")

r <- raster("wheat_winter-wheat_stdev_yield_.asc") 
plot(r)
title(main="stdev WW yield")

############

r <- raster("sugar-beet__avg_yield_.asc") 
plot(r)
title(main="avg sugarbeet yield")

r <- raster("sugar-beet__stdev_yield_.asc") 
plot(r)
title(main="st dev sugarbeet yield")

###################

r <- raster("maize_silage-maize_avg_yield_.asc") 
plot(r)
title(main="avg silage maize yield")

###################

r <- raster("barley_winter-barley_avg_yield_.asc") 
plot(r)
title(main="avg winter barley yield")

##################
#Poster SUSTAg
r <- raster("allcrops_avg_pot_residues_.asc") 
plot(r,
     axes=FALSE)
title(main="Potential residue yield (kg ha-1 year-1)", cex.main=2)

r <- raster("allcrops_avg_ExportResidues_.asc") 
plot(r,
     axes=FALSE)
title(main="Removed residues (kg ha-1 year-1)", cex.main=2)

r <- raster("yearly_avg_deltaOC_.asc") 
plot(r,
     axes=FALSE)
title(main="Relative SOC change in topsoil (% year-1)", cex.main=2)

r <- raster("yearly_avg_Nleach_.asc") 
plot(r,
     axes=FALSE)
title(main="Nitrogen leaching (kg N ha-1 year-1)", cex.main=2)

######################
#Kuopio meeting
r <- raster("yearly_avg_deltaOC_0removal.asc") 
plot(r, col=rev(terrain.colors(99)), breaks=seq(-3, 3, length.out=100), legend=F)
plot(r, col=rev(terrain.colors(6)), breaks=seq(-3, 3, length.out=7), legend.only=T)
title(main="no residue removal")

r <- raster("yearly_avg_deltaOC_33removal.asc") 
plot(r, col=rev(terrain.colors(99)), breaks=seq(-3, 3, length.out=100), legend=F)
plot(r, col=rev(terrain.colors(6)), breaks=seq(-3, 3, length.out=7), legend.only=T)
title(main="33% residue removal")

r <- raster("yearly_avg_deltaOC_100removal.asc") 
plot(r, col=rev(terrain.colors(99)), breaks=seq(-3, 3, length.out=100), legend=F)
plot(r, col=rev(terrain.colors(6)), breaks=seq(-3, 3, length.out=7), legend.only=T)
title(main="100% residue removal")

r <- raster("yearly_avg_ini_SOCtop_.asc") 
plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 3, length.out=100), legend=F)
plot(r, col=rev(terrain.colors(6)), breaks=seq(-3, 3, length.out=7), legend.only=T)
title(main="ini SOC")
