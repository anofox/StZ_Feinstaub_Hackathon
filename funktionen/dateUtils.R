fillDateGaps <- function(dtData, dateVar, samplingRate = 3600) {
  dtData[[dateVar]] <- as.POSIXct(dtData[[dateVar]])
  startDate <- min(dtData[[dateVar]], na.rm = T)
  endDate <- max(dtData[[dateVar]], na.rm = T)
  dateSeq <- seq(from = startDate, to = endDate, by = samplingRate)
  dtTime <- data.table::data.table(dt = dateSeq)
  data.table::setnames(dtTime, "dt", dateVar)
  data.table::setkeyv(dtTime, dateVar)
  data.table::setkeyv(dtData, dateVar)
  dtData[dtTime]
}
