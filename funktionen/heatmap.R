#' @title Creates a time series heatmap
#' 
#' @description Public version of a time series heatmap
#' 
#' @param dtData a time series data.frame/data.table with columns timestamp 
#' (POSIXct format), value (numeric), and sensor_id
#' @param title title of the plot
#' @param doNormalize normalize sensor time series
#' @param doCutZero cut negative values
#' 
#' @return a ggplot2 object
#' 
#' @export
heatmapTS <- function(dtData, title = "", doNormalize = T, doCutZero = F) {
  if (!is(dtData, "data.table"))
    dtData <- data.table::as.data.table(dtData)
  dtData[, minDate := min(timestamp), by = sensor_id]
  data.table::setkey(dtData, minDate)
  
  # normalize values some series have high values
  if (doNormalize) { 
    dtData[, value := value / max(value, na.rm=T), by = sensor_id]
  }
  
  # cut negative values
  if (doCutZero) {
    dtData[value < 0, value := NA]
  }
  
  # keep minDate sorting
  dtData[, id:=paste0("sensor_", sensor_id)]
  lev <- unique(dtData$id)
  dtData[, id := factor(id, levels = lev)]
  ggplot(dtData) +
    geom_tile(aes(x = timestamp, y = id, fill = value), na.rm = T) +
    scale_fill_viridis(name=NULL, option ="C", na.value="white") +
    theme_bw() + xlab("Zeitstempel") + ylab("Sensor ID") +
    ggtitle(title) +
    theme(legend.position = "none",
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank(),
          panel.grid.major.y = element_blank(),
          panel.grid.minor.y = element_blank()) -> p
  p
}