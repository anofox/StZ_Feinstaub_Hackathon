library(data.table)
library(tidyverse)
library(fst)


# 1.0 Reliability of Sensors ----
#' Temperature seems to be a good measure for selecting reliable sensors
#' 1. the temperature curve looks quite similar over all dwd stations
#' 2. there should not be a too great variation in the data
#' 
#' PLEASE look at: Datenqualität_-_KPI.html for a detailed explanation


# 1.1 load dwd data ----
fst::read_fst("./data/temperatureData.fst") %>% 
  as.data.table() -> dwdTemperature
fst::read_fst("./data/dwdFilteredSensorData.fst") %>% 
  as.data.table() -> dwdStations

# 1.2 load sensor data ----
fst::read_fst("./data/sensorsWithDWD.fst") %>% 
  as.data.table() -> sensors
fst::read_fst("./data/filteredStgtFeinstaub.fst") %>% 
  as.data.table() -> sensorData

# 1.3 data preparation for temperature analysis ----
sensorData %>% 
  dplyr::filter(variable == "temperature") %>% 
  dplyr::select(sensor_id, timestamp, value) %>% 
  data.table::as.data.table() -> sensorDataTemp
sensors %>% 
  dplyr::select(sensor_id, closestTemp) -> sensorsTemp

# 1.4 add closest dwd station id to Feinstaub time series data ----
sensorDataTemp %>% 
  dplyr::left_join(sensorsTemp, "sensor_id") %>% 
  dplyr::rename(dwd_id = closestTemp) %>% 
  data.table::as.data.table() -> sensorDataTemp

# 1.5 add dwd temperature timeseries data to Feinstaub temperature data ----
dwdTemperature %>% 
  dplyr::select(STATIONS_ID, TT_TU, date) %>% 
  data.table::as.data.table() -> dwdTemperature
setnames(dwdTemperature, c("STATIONS_ID", "date"), c("dwd_id", "timestamp"))
sensorDataTemp %>% 
  dplyr::filter(!is.na(dwd_id)) %>% 
  dplyr::left_join(dwdTemperature, c("dwd_id", "timestamp")) %>% 
  data.table::as.data.table() -> sensorDataTemp


# 1.6 calculate difference between DWD temperature and Feinstaub temperature ----
sensorDataTemp[TT_TU == -999, TT_TU := NA]
sensorDataTemp[, diff := value - TT_TU]
summary(sensorDataTemp)


# 2.0 Calculations ----
# Following images are not shown on the Hackathon presentation
# Calculate Root Mean Squared Error ----

# Helper function
f <- function(x) {
  mean(abs(x)^2, na.rm = T)
}
rmseTemp <- sensorDataTemp[, lapply(.SD, f), .SDcols = c("diff"), by = "sensor_id"]
data.table::setkey(rmseTemp, diff)
rmseTemp


# 2.1 Extract the ten worst Feinstaub sensors according to the RMSE ----
sensorDataTempExp <- reshape2::melt(sensorDataTemp[sensor_id %in% rmseTemp[(.N-10):.N]$sensor_id, c("sensor_id", "timestamp", "value", "TT_TU")], id.vars = c("sensor_id", "timestamp"))
sensorDataTempExp %>% 
  dplyr::mutate(variable = ifelse(variable == "value", "Luftinfo Station", "DWD Station")) -> sensorDataTempExp
p <- ggplot(sensorDataTempExp) +
  geom_line(aes(x = timestamp, y = value, group = variable, color = variable), alpha = .5) +
  theme_bw() +
  scale_color_viridis_d(name = NULL) +
  theme(legend.position = "top") +
  facet_wrap(~sensor_id, scales = "free") + 
  ylab("Temperatur") + 
  ggtitle("Ten worst Measurements According to RMSE")
ggsave(filename = "images/DQ_Temperature_Worst_RMSE.png", plot = p, width = 10, height = 10)  


# 2.2 Extract the ten best Feinstaub sensors according to the RMSE ----
sensorDataTempExp <- reshape2::melt(sensorDataTemp[sensor_id %in% na.omit(rmseTemp)[1:10, ]$sensor_id, c("sensor_id", "timestamp", "value", "TT_TU")], id.vars = c("sensor_id", "timestamp"))
sensorDataTempExp %>% 
  dplyr::mutate(variable = ifelse(variable == "value", "Luftinfo Station", "DWD Station")) -> sensorDataTempExp
p <- ggplot(sensorDataTempExp) +
  geom_line(aes(x = timestamp, y = value, group = variable, color = variable), alpha = .5) +
  theme_bw() +
  scale_color_viridis_d(name = NULL) +
  theme(legend.position = "top") +
  facet_wrap(~sensor_id, scales = "free") + 
  ylab("Temperatur") + 
  ggtitle("Ten Best Measurements According to RMSE")
p
ggsave(filename = "images/DQ_Temperature_Best_RMSE.png", plot = p, width = 10, height = 10)  
