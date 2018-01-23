getLocationDataWide <- function(locID = 2483, sensorData, sensorStations, dwdData) {
  sensorStations %>% 
    dplyr::filter(location == locID) -> locSensors
  sensorData %>% 
    dplyr::filter(sensor_id %in% locSensors$sensor_id) %>% 
    dplyr::select(timestamp, variable, value) %>% 
    data.table::as.data.table() %>% 
    data.table::dcast(timestamp ~ variable, value.var = "value") -> locData
  
  statID <- c(locSensors$closestPerc, locSensors$closestCloud, locSensors$closestSun, locSensors$closestWind, locSensors$closestTemp)
  statID <- unique(statID)
  # get dwd data
  dwdData %>% 
    dplyr::mutate(variable = paste0(variable, "DWD")) %>% 
    dplyr::filter(STATIONS_ID %in% statID) %>% 
    dplyr::select(-STATIONS_ID) %>% 
    data.table::as.data.table() -> dwdLocData
  dwdLocData[, startDate := min(timestamp), by = variable]
  filterDate <- max(dwdLocData$startDate)
  dwdLocData <- dwdLocData[timestamp >= filterDate]
  dwdLocData[, startDate := NULL]
  dwdLocData %>% 
    data.table::dcast(timestamp ~ variable, fun.aggregate = mean, value.var = "value") -> dwdLocData
  
  # join data
  locData %>% 
    dplyr::left_join(dwdLocData, "timestamp") %>% 
    data.table::as.data.table() -> locData
  setkeyv(locData, "timestamp")
  
  locData
}