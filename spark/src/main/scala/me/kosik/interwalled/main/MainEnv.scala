package me.kosik.interwalled.main

import org.apache.spark.sql.SparkSession

case class MainEnv(
  sparkMaster: String,
  driverMemory: String,
  executorMemory: String,
  executorInstances: Int,
  executorCores: Int,
  dataDirectory: String
) {
  
  def buildSparkSession(applicationName: String): SparkSession = {
    SparkSession.builder()
      .appName(applicationName)
      .config("spark.driver.memory",      driverMemory)
      .config("spark.executor.memory",    executorMemory)
      .config("spark.executor.instances", executorInstances)
      .config("spark.executor.cores",     executorCores)
      .master(sparkMaster)
      .getOrCreate()
  }
}

object MainEnv {
  def build(): MainEnv =
    MainEnv.build(sys.env)

  def build(envVariables: Map[String, String]): MainEnv = MainEnv(
    sparkMaster       = envVariables.getOrElse("INTERWALLED_SPARK_MASTER",              "local[*]"),
    driverMemory      = envVariables.getOrElse("INTERWALLED_SPARK_DRIVER_MEMORY",       "4G"),
    executorMemory    = envVariables.getOrElse("INTERWALLED_SPARK_EXECUTOR_MEMORY",     "6G"),
    executorInstances = envVariables.getOrElse("INTERWALLED_SPARK_EXECUTOR_INSTANCES",  "4").toInt,
    executorCores     = envVariables.getOrElse("INTERWALLED_SPARK_EXECUTOR_CORES",      "1").toInt,
    dataDirectory     = envVariables.getOrElse("INTERWALLED_DATA_DIRECTORY",            "data")
  )
}