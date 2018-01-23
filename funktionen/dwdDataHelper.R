#' @title Get all stations 
#' 
#' @description Get all DWD stations
#' 
#' @param sc a spark session object
#' @param path path to parquet files
#' 
#' @return a data.frame with all DWD stations around Stuttgart
#' 
#' @export
getAllDWDStations <- function(sc, path = "../dwd") {
  # get all stations
  sparklyr::spark_read_parquet(sc, name = "windDF", path = path.expand(path, "wind.parquet")) %>% 
    getStation() -> windStations
  sparklyr::spark_read_parquet(sc, name = "sunDF", path = path.expand(path, "sun.parquet")) %>% 
    getStation() -> sunStations
  sparklyr::spark_read_parquet(sc, name = "cloudDF", path = path.expand(path, "cloudiness.parquet")) %>% 
    getStation() -> cloudStations
  sparklyr::spark_read_parquet(sc, name = "percDF", path = path.expand(path, "precipitation.parquet")) %>% 
    getStation() -> precipitationStations
  sparklyr::spark_read_parquet(sc, name = "tempDF", path = path.expand(path, "temp.parquet")) %>% 
    getStation() -> tempStations
  
  rbind(windStations, sunStations, cloudStations, precipitationStations, tempStations) %>% 
    distinct() -> allStations
  
  # add column related to available data
  windStations %>% 
    dplyr::select(STATIONS_ID) %>% 
    dplyr::mutate(isWind = 1L) %>% 
    dplyr::right_join(allStations, "STATIONS_ID") %>% 
    dplyr::mutate_at(funs(replace(., which(is.na(.)), 0L)), .vars = "isWind") -> allStations
  
  sunStations %>% 
    dplyr::select(STATIONS_ID) %>% 
    dplyr::mutate(isSun = 1L) %>% 
    dplyr::right_join(allStations, "STATIONS_ID") %>% 
    dplyr::mutate_at(funs(replace(., which(is.na(.)), 0L)), .vars = "isSun") -> allStations
  
  cloudStations %>% 
    dplyr::select(STATIONS_ID) %>% 
    dplyr::mutate(isCloud = 1L) %>% 
    dplyr::right_join(allStations, "STATIONS_ID") %>% 
    dplyr::mutate_at(funs(replace(., which(is.na(.)), 0L)), .vars = "isCloud") -> allStations
  
  precipitationStations %>% 
    dplyr::select(STATIONS_ID) %>% 
    dplyr::mutate(isPerc = 1L) %>% 
    dplyr::right_join(allStations, "STATIONS_ID") %>% 
    dplyr::mutate_at(funs(replace(., which(is.na(.)), 0L)), .vars = "isPerc") -> allStations
  
  tempStations %>% 
    dplyr::select(STATIONS_ID) %>% 
    dplyr::mutate(isTemp = 1L) %>% 
    dplyr::right_join(allStations, "STATIONS_ID") %>% 
    dplyr::mutate_at(funs(replace(., which(is.na(.)), 0L)), .vars = "isTemp") -> allStations
  
  # save as parquet
  allStationsDF <- sparklyr::sdf_copy_to(sc, allStations)
  sparklyr::spark_write_parquet(allStationsDF, 
                                path.expand(path, "AllStations.parquet"), 
                                mode = "overwrite")
  
  # save as fst
  fst::write_fst(allStations, path = "./data/dwdStations.fst")
  
  # return object
  allStations
}


#' @title Parquet to FST R format
#' 
#' @description Transform dwd from parquet to fst, add date and filter 
#' date > 2016-01-01
#' 
#' @param sc a spark session object
#' @parm actualStations a data.frame with avilable stations. This information is
#' used to pre-filter data
#' 
#' @return nothing
#' 
#' @export
parquetToFSTDWD <- function(sc, actualStations) {
  # Get Wind Data
  windDF <- sparklyr::spark_read_parquet(sc, name = "windDF", path = "../dwd/wind.parquet/")
  windDF %>% 
    dplyr::collect() %>% 
    dwdDate() %>% 
    dplyr::filter(STATIONS_ID %in% actualStations$STATIONS_ID) -> windData
  fst::write_fst(windData, "./data/windData.fst")
  rm(windData)
  
  # Get Sun Data
  sunDF <- sparklyr::spark_read_parquet(sc, name = "sunDF", path = "../dwd/sun.parquet/")
  sunDF %>% 
    dplyr::collect() %>% 
    dwdDate() %>% 
    dplyr::filter(STATIONS_ID %in% actualStations$STATIONS_ID) -> sunDData
  fst::write_fst(sunDData, "./data/sunData.fst")
  rm(sunDData)
  
  # Get Cloud Data
  cloudDF <- sparklyr::spark_read_parquet(sc, name = "cloudDF", path = "../dwd/cloudiness.parquet/")
  cloudDF %>% 
    dplyr::collect() %>% 
    dwdDate() %>% 
    dplyr::filter(STATIONS_ID %in% actualStations$STATIONS_ID) -> cloudDData
  fst::write_fst(cloudDData, "./data/cloudData.fst")
  rm(cloudDData)
  
  # Percipitation
  percDF <- sparklyr::spark_read_parquet(sc, name = "percDF", path = "../dwd/precipitation.parquet/")
  percDF %>% 
    dplyr::collect() %>% 
    dwdDate() %>% 
    dplyr::filter(STATIONS_ID %in% actualStations$STATIONS_ID) -> percData
  fst::write_fst(percData, "./data/precipitationData.fst")
  rm(percData)
  
  # Temperature
  tempDF <- sparklyr::spark_read_parquet(sc, name = "tempDF", path = "../dwd/temp.parquet/")
  tempDF %>% 
    dplyr::collect() %>% 
    dwdDate() %>% 
    dplyr::filter(STATIONS_ID %in% actualStations$STATIONS_ID) -> tempData
  fst::write_fst(tempData, "./data/temperatureData.fst")
  rm(tempData)
}


#' transform numeric to apropriate date format
dwdDate <- function(df) {
  df <- data.table::as.data.table(df)
  df[, date := as.POSIXct(strptime(MESS_DATUM, format = "%Y%m%d%H"))]
  df[date >= as.Date("2016-01-01")]
}


#' @title Get unique DWD Stations with actual measurement values
#' 
#' @param df a data.frame
#' 
#' @return a data.frame with unqiue stations
getStation <- function(df) {
  df %>% 
    dplyr::group_by(STATIONS_ID) %>% 
    dplyr::summarise(actualDate = max(MESS_DATUM, na.rm=T)) %>% 
    collect() %>% 
    dplyr::mutate(date := as.POSIXct(strptime(actualDate, format = "%Y%m%d%H"))) %>% 
    dplyr::filter(as.Date(date) >= as.Date("2018-01-01")) -> stationDatum
  
  df %>% 
    dplyr::filter(STATIONS_ID %in% stationDatum$STATIONS_ID) %>% 
    dplyr::select(STATIONS_ID, geoBreite, geoLaenge, Stationshoehe, Stationsname) %>% 
    dplyr::distinct() %>% 
    dplyr::collect() %>% 
    dplyr::mutate(Stationsname = stringr::str_trim(Stationsname))
}
