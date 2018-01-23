library(McSpatial)
library(tidyverse)

#' You need to apply functions:
#' 1. 00_dataPreparationDWD.R and
#' 2. 00_dataPreparationFeinstaub.R
#' before executing this code


# 1.0 get for all locations the closest dwd station ----
# load prepared data
feinstaubStations <- fst::read_fst("./data/sensorsStgt.fst")
dwdStations <- fst::read_fst("./data/dwdStations.fst")


# get for each feinstaub location the closest DWD station
distMatrix <- c()
for (i in 1:nrow(dwdStations)) {
  dist <- geodistance(longvar = feinstaubStations$lon, 
                      latvar = feinstaubStations$lat, 
                      lotarget = dwdStations$geoLaenge[i], 
                      latarget = dwdStations$geoBreite[i], dcoor = FALSE)$dist
  # miles to km
  dist <- dist * 1.60934
  distMatrix <- cbind(distMatrix, dist)
}
heatmap(distMatrix)

# Calculate closest DWD station for a given dimension
feinstaubStations %>% 
  getClosestDWD(dwdStations, "Perc") %>% 
  getClosestDWD(dwdStations, "Cloud") %>% 
  getClosestDWD(dwdStations, "Sun") %>% 
  getClosestDWD(dwdStations, "Wind") %>% 
  getClosestDWD(dwdStations, "Temp") -> feinstaubStations
fst::write_fst(feinstaubStations, path = "./data/sensorsWithDWD.fst")


# 2.0 Calculate location distance matrix ----
# Calculate the distance between Feinstaub station and save it as a matrix
# This information is not used in the Hackathon and may be used in further
# analysis
feinstaubStations <- fst::read_fst("./data/sensors_stgt.fst")

feinstaubStations %>% 
  dplyr::select(location, lon, lat) %>% 
  unique() -> locations

distMatrix <- c()
for (i in 1:nrow(locations)) {
  dist <- geodistance(longvar = locations$lon, 
                      latvar = locations$lat, 
                      lotarget = locations$lon[i], 
                      latarget = locations$lat[i], dcoor = FALSE)$dist
  # miles to km
  dist <- dist * 1.60934
  distMatrix <- cbind(distMatrix, dist)
}

rownames(distMatrix) <- locations$location
colnames(distMatrix) <- locations$location
fst::write_fst(as.data.frame(distMatrix), path = "./data/locationDistanceMatrixFeinstaub.fst")
