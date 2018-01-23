library(tidyverse)
library(data.table)

#' Plots used for understanding the data a bit better. 
#' PLEASE look at: Datenqualität_-_KPI.html for a detailed explanation


# 1.0 Plot Precipitation ----
fst::read_fst("data/precipitationData.fst") %>% 
  data.table::as.data.table() -> precipitation
precipitation[R1 == -999, R1 := NA]

ggplot(precipitation) +
  geom_line(aes(x = date, y = R1, color = Stationsname)) +
  theme_bw()

# 2.0 Plot Temperature ----
fst::read_fst("data/temperatureData.fst") %>% 
  data.table::as.data.table() -> tempDWD
tempDWD[TT_TU == -999, TT_TU := NA]
ggplot(tempDWD) +
  geom_line(aes(x = date, y = TT_TU, color = Stationsname)) +
  theme_bw()
