package me.kosik.interwalled.test.data.generator


case class MainEnv(
  sparkMaster: String,
  driverMemory: String,
  executorMemory: String,
  dataDirectory: String
)

object MainEnv {
  def build(): MainEnv =
    MainEnv.build(sys.env)

  def build(envVariables: Map[String, String]): MainEnv = MainEnv(
    sparkMaster     = envVariables.getOrElse("INTERWALLED_SPARK_MASTER",            "local[*]"),
    driverMemory    = envVariables.getOrElse("INTERWALLED_SPARK_DRIVER_MEMORY",     "4G"),
    executorMemory  = envVariables.getOrElse("INTERWALLED_SPARK_EXECUTOR_MEMORY",   "4G"),
    dataDirectory   = envVariables.getOrElse("INTERWALLED_DATA_DIRECTORY",          "data")
  )
}