library(data.table)
library(ggplot2)
library(viridis)
library(dplyr)
library(BBmisc)

# Custom Functions
source("funktionen/heatmap.R")


# 1.0 Load Data ----
# load sensor meta data
fst::read_fst("../LuftinfoApp/data/sensors_stgt_with_dwd.fst") %>% 
  as.data.table() -> sensors
# load time series data
fst::read_fst("../LuftinfoApp/data/shiny_heuristic_luft_stgt.fst") %>% 
  as.data.table() -> sensorData


# 1.1 Create Heatmaps ----
# P1 Heatmap
p1Data <- sensorData[variable == "P1"]
p <- heatmapTS(p1Data, "P1", T, T)
ggsave("Images/heatmapP1.pdf", p, dpi = 300, width = 12, height = 12)  

# P2 Heatmap
p2Data <- sensorData[variable == "P2"]
p <- heatmapTS(p2Data, "P2", T, T)
ggsave("Images/heatmapP2.png", p, dpi = 300, width = 6, height = 6)  

# Temperature Heatmap
tempData <- sensorData[variable == "temperature"]
p <- heatmapTS(tempData, "Temperatur", T, F)
ggsave("Images/heatmapTemp.png", p, dpi = 300, width = 6, height = 6)  

# Humidity Heatmap
humData <- sensorData[variable == "humidity"]
p <- heatmapTS(humData, "Humiditiy", T, F)
ggsave("Images/heatmapHumidity.png", p, dpi = 300, width = 6, height = 6)  
