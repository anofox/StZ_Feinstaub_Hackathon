getSparkConfig <- function() {
  config <- sparklyr::spark_config()
  config$spark.executor.memory <- "16G"
  config$spark.driver.memory <- "16G"
  config[['sparklyr.shell.driver-memory']] <- "4G"
  config[['sparklyr.shell.executor-memory']] <- "4G"
  config$spark.yarn.executor.memoryOverhead <- "1g"
  config
}