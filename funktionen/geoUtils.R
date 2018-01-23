#' @title clean multiple geo tags (list from python)
cleanGeo <- function(x) {
  x %>% 
    stringr::str_replace_all("[\\[\\]]", "") %>% 
    stringr::str_split(pattern = ",") %>% 
    purrr::reduce(c) -> x
  n <- length(x)  
  as.numeric(x[n])
}


#' @title Calculates closes DWD station for a given Feinstaub station
#' 
#' @description Not all DWD stations have all sensor data. Therefore, each 
#' dimension gets its own column.
#' 
#' @param feinstaubStations data.frame with Feinstaub stations
#' @param dwdStations data.frame with DWD stations
#' @param dim dimesniosn to join, e.g. temperature
#' 
#' @return a data.frame with Feinstaub stations and added closest DWD station
#' for that given dimension
#' 
#' @export
getClosestDWD <- function(feinstaubStations, dwdStations, dim) {
  isDim <- paste0("is", dim)
  dimTarget <- paste0("closest", dim)
  id <- which(dwdStations[[isDim]] == 1)
  dwdStations %>% 
    dplyr::filter_(paste0(isDim, " == 1")) %>% 
    .[["STATIONS_ID"]] -> stationId
  closest <- apply(distMatrix[, id], 1, which.min)
  feinstaubStations[[dimTarget]] <- stationId[closest]
  feinstaubStations
}