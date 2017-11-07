
#https://stackoverflow.com/questions/9436947/legend-properties-when-legend-only-t-raster-package
#http://neondataskills.org/R/Plot-Rasters-In-R/


# Clear Environment Variables
rm(list = ls())

library(raster)
library(rgdal)

setwd("C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/grids")

#VOCE grids
r <- raster("maize_grain-maize_avg_sumJMono_.asc") 
plot(r)
title(main="avg maize sumJMono")

r <- raster("maize_grain-maize_avg_sumGMono_.asc") 
plot(r)
title(main="avg maize sumGMono")

r <- raster("maize_grain-maize_avg_maxLAI_.asc") 
plot(r)
title(main="avg maize max LAI")


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
title(main="avg delta OC (yr-1)")

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

r <- raster("yearly_avg_deltaOC_.asc") 
plot(r,
     axes=FALSE)
title(main="Relative SOC change in topsoil (% year-1)", cex.main=2)

r <- raster("yearly_avg_Nleach_.asc") 
plot(r,
     axes=FALSE)
title(main="Nitrogen leaching (kg N ha-1 year-1)", cex.main=2)

###Laura Klemens grids
r <- raster("maize_silage-maize_avg_Nleach_.asc") 
plot(r)
title(main="avg silage maize Nleach Nmin method")

r <- raster("barley_winter-barley_avg_Nleach_.asc") 
plot(r)
title(main="avg winter barley Nleach Nmin method")

r <- raster("sugar-beet__avg_Nleach_.asc") 
plot(r)
title(main="avg sugar beet Nleach Nmin method")

r <- raster("rape_winter-rape_avg_Nleach_.asc") 
plot(r)
title(main="avg winter rape Nleach Nmin method")

r <- raster("maize_grain-maize_avg_Nleach_.asc") 
plot(r)
title(main="avg grain maize Nleach Nmin method")

r <- raster("potato_moderately-early-potato_avg_Nleach_.asc") 
plot(r)
title(main="avg potato Nleach Nmin method")

r <- raster("wheat_winter-wheat_avg_Nleach_.asc") 
plot(r)
title(main="avg winter wheat Nleach Nmin method")

r <- raster("barley_spring-barley_avg_Nleach_.asc") 
plot(r)
title(main="avg spring barley Nleach Nmin method")

r <- raster("triticale_winter-triticale_avg_Nleach_.asc") 
plot(r)
title(main="avg winter triticale Nleach Nmin method")
