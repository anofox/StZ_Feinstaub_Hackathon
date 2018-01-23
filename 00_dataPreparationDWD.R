library(sparklyr)
library(data.table)
library(tidyverse)
library(fst)
library(reshape2)

# Custom functions
source("funktionen/sparklyr.R")
source("funktionen/dwdDataHelper.R")


# 1.0 Init Spark ----
# This code gets all unique DWD weather stations around Stuttgart (pre-filtered)
# and transforms parquet data to the fst data format which is directly readable 
# in R
# spark_install(version = "2.2.0")
config <- getSparkConfig()
sc <- spark_connect(master = "local", config = config)

# 1.1 Get all DWD Stations in Stuttgart and Mark them ----
allDWDStations <- getAllDWDStations(sc)

# 1.2 Filter DWD Stations & Transform DWD Parquet to FST ----
parquetToFSTDWD(sc, allDWDStations)
spark_disconnect(sc)


# 2.0 merge dwd data ----
# get DWD time series data and merge data with sensor information. Furthermore,
# we just use data after 01.01.2016
# 2.1 Temperatur ---
fst::read_fst("./data/temperatureData.fst") %>% 
  dplyr::select(STATIONS_ID, TT_TU, date) %>% 
  dplyr::rename(timestamp = date,
                temperature = TT_TU) %>% 
  dplyr::mutate(temperature = ifelse(temperature == -999, NA, temperature)) %>% 
  data.table::as.data.table() %>% 
  data.table::melt(id.vars = c("STATIONS_ID", "timestamp")) -> dwdTemperature

# 2.2 Precipitation ----
fst::read_fst("./data/precipitationData.fst") %>% 
  dplyr::select(STATIONS_ID, R1, date) %>% 
  dplyr::rename(timestamp = date,
                precipitation = R1) %>% 
  dplyr::mutate(precipitation = ifelse(precipitation == -999, NA, precipitation)) %>% 
  data.table::as.data.table() %>% 
  data.table::melt(id.vars = c("STATIONS_ID", "timestamp")) -> dwdPrecipitation

# 2.3 Wind ----
fst::read_fst("./data/windData.fst") %>% 
  dplyr::select(STATIONS_ID, F, D, date) %>% 
  dplyr::rename(timestamp = date,
                windSpeed = F,
                windDirection = D) %>% 
  dplyr::mutate(windSpeed = ifelse(windSpeed == -999, NA, windSpeed),
                windDirection = ifelse(windDirection == -999, NA, windDirection)) %>% 
  data.table::as.data.table() %>% 
  data.table::melt(id.vars = c("STATIONS_ID", "timestamp")) -> dwdWind

# 2.4 SUN ----
fst::read_fst("data/sunData.fst") %>% 
  dplyr::select(STATIONS_ID, SD_SO, date) %>% 
  dplyr::rename(timestamp = date,
                sun = SD_SO) %>% 
  dplyr::mutate(sun = ifelse(sun == -999, NA, sun)) %>% 
  data.table::as.data.table() %>% 
  data.table::melt(id.vars = c("STATIONS_ID", "timestamp")) -> dwdSun

# 2.4 Cloud ----
fst::read_fst("data/cloudData.fst") %>% 
  dplyr::select(STATIONS_ID, V_N, date) %>% 
  dplyr::rename(timestamp = date,
                cloud = V_N) %>% 
  dplyr::mutate(cloud = ifelse(cloud == -999, NA, cloud)) %>% 
  data.table::as.data.table() %>% 
  data.table::melt(id.vars = c("STATIONS_ID", "timestamp")) -> dwdCloud

# 2.5 Merge, filter, and save Data ----
dwdData <- rbind(dwdTemperature, dwdPrecipitation, dwdWind, dwdSun, dwdCloud)
dwdData <- dwdData[as.Date(timestamp) >= as.Date("2016-01-01")]
fst::write_fst(dwdData, path = "./data/dwdFilteredSensorData.fst")
