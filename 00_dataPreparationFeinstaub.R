library(dplyr)
library(purrr)
library(fst)
library(reshape2)
library(data.table)
library(sparklyr)
library(dplyr)
library(e1071)

# Custom functions
source("funktionen/geoUtils.R")
source("funktionen/dateUtils.R")
source("funktionen/sparklyr.R")


# 1.0 Data Preparation ----
# 1.1 Sensors ----
sensorStations <- readr::read_csv("../Luftdaten/unique_sensors.csv")
sensorStations %>% 
  dplyr::select(-X1) %>% 
  dplyr::filter(sensor_type %in% c("SDS011", "DHT22")) -> sensorStations

sensorStations$lon %>% 
  purrr::map(.f = cleanGeo) %>% 
  purrr::reduce(c) -> sensorStations$lon
sensorStations$lat %>% 
  purrr::map(.f = cleanGeo) %>% 
  purrr::reduce(c) -> sensorStations$lat
# save data later


# 1.2 Load Sensor Timeseries Data ----
# configure SPARK
config <- getSparkConfig()
sc <- spark_connect(master = "local", config = config)

# read parquet file
sensorDataDF <- spark_read_parquet(sc, "sensorDataDF", "../Luftdaten/stgt_hourly_mean.parquet/")

# collect data from JVM to R
sensorDataDF %>% 
  dplyr::collect() -> sensorData

# close SPARK connection
spark_disconnect(sc)

# reformat timeseries data to long format
sensorData %>% 
  reshape2::melt(id.vars = c("sensor_id", "timestamp")) %>% 
  na.omit() -> sensorData


# 1.2.1 add sensor information ----
sensorData %>% 
  dplyr::filter(sensor_id %in% sensorStations$sensor_id) %>% 
  dplyr::left_join(sensorStations, "sensor_id") %>% 
  dplyr::mutate(value = as.numeric(value)) %>% 
  dplyr::arrange(sensor_id, timestamp) %>% 
  data.table::as.data.table() -> sensorData


# 1.2.2 heuristic data cleaning ----
# delete non actual sensors, sensors with too less measurements, and sensors
# with no variance; ACTUAL DATE SHOULD BE REPLACED
actualDate <- as.Date("2018-01-01")
sensorData %>% 
  dplyr::group_by(sensor_id) %>% 
  dplyr::summarise(
    min = min(timestamp, na.rm = T) %>% as.Date(),
    max = max(timestamp, na.rm = T) %>% as.Date(),
    n   = length(timestamp),
    sd  = sd(value, na.rm = T)
  ) %>% 
  dplyr::filter(
    max == actualDate, 
    n > 200, 
    sd > 0
    ) -> usableSensors

# keep usable sensors
sensorData %>% 
  dplyr::filter(sensor_id %in% usableSensors$sensor_id) -> sensorData
sensorStations %>% 
  dplyr::filter(sensor_id %in% usableSensors$sensor_id) -> sensorStations


# 1.3 fill missing time stamps with NA values ----
sensors <- unique(sensorData[["sensor_id"]])
sensorData <- data.table::as.data.table(sensorData)
data.table::setkey(sensorData, sensor_id)
sensors %>% 
  purrr::map(.f = function(x, dtData) {
    singleSensorData <- dtData[sensor_id == x]
    dimVars <- unique(singleSensorData[["variable"]])
    
    singleSensorData %>% 
      dplyr::select(sensor_id, location, sensor_type, lon, lat) %>% 
      unique() %>% 
      na.omit() -> sensor
    
    singleSensorData <- singleSensorData[, c("timestamp", "variable", "value")]
    
    singleSensorData[variable == dimVars[1]] %>% 
      dplyr::select(timestamp, value) %>% 
      fillDateGaps(dateVar = "timestamp") -> singleSensorDataLvl1
    singleSensorDataLvl1[, variable := dimVars[1]]
    
    singleSensorData[variable == dimVars[2]] %>% 
      dplyr::select(timestamp, value) %>% 
      fillDateGaps(dateVar = "timestamp") -> singleSensorDataLvl2
    singleSensorDataLvl2[, variable := dimVars[2]]
    
    singleSensorData <- rbind(singleSensorDataLvl1, singleSensorDataLvl2)
    singleSensorData[, sensor_id := sensor$sensor_id]
    singleSensorData %>% 
      dplyr::left_join(sensor, "sensor_id") %>% 
      data.table::as.data.table()
  }, dtData = sensorData) -> sensorData


# 1.4 Save data ----
# save sensor datasets
sensorData %>% 
  purrr::reduce(rbind) -> sensorData
fst::write_fst(sensorData, path = "./data/filteredStgtFeinstaub.fst", compress = 100)
sensorStations %>% 
  dplyr::filter(sensor_id %in% unique(sensorData$sensor_id)) -> sensorStations
fst::write_fst(sensorStations, path = "./data/sensorsStgt.fst", compress = 100)


# 1.2.3 Primitive Statistical Data Quality ----
# this is also done in Shiny interactive
library(dplyr)
library(tsfeatures)
sensorData <- fst::read_fst(path = "./data/filteredStgtFeinstaub.fst")
sensorData %>% 
  dplyr::group_by(location, sensor_type, sensor_id, variable) %>% 
  dplyr::summarise(
    mw              = mean(value, na.rm = T),
    sd              = sd(value, na.rm = T),
    skewness        = e1071::skewness(value, na.rm = T),
    kurtosis        = e1071::kurtosis(value, na.rm = T),
    entropy         = entropy(value),
    crossing_points = crossing_points(value),
    flat_spots      = flat_spots(value),
    acf1            = acf_features(value)[1],
    lumpiness       = lumpiness(value, 48),
    stability       = stability(value, 48),
    max_kl_shift    = max_kl_shift(value, 48)[1],
    max_kl_index    = max_kl_shift(value, 48)[2],
    max_level_shift = max_level_shift(value, 48)[1],
    max_level_index = max_level_shift(value, 48)[2],
    max_var_shift   = max_var_shift(value, 48)[1],
    max_var_index   = max_var_shift(value, 48)[2],
    arch_stat       = arch_stat(value, lags = 24, T),
    holt_alpha      = holt_parameters(value)[1],
    holt_beta       = holt_parameters(value)[2]
  ) %>% 
  dplyr::ungroup() -> stgtKPI
stgtKPI
fst::write_fst(stgtKPI, path = "./data/stgtFullFeinstaubKPIs", compress = 100)
