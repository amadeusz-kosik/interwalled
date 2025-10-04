package me.kosik.interwalled.benchmark.app

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

import scala.concurrent.duration.Duration


case class MainEnv(
  sparkSession: SparkSession,
  csvDirectory: String,
  dataDirectory: String,
  timeoutAfter: Duration
)


object MainEnv {

  def build(applicationName: String): MainEnv =
    MainEnv.build(applicationName, sys.env)

  def build(applicationName: String, envVariables: Map[String, String]): MainEnv = {
    val sparkSession = {
      SparkSession.builder()
        .appName(applicationName)
        .config("spark.task.maxFailures", 1)
        .getOrCreate()
    }

    // Turn off noisy loggers
    Array(
      Logger.getLogger("org"),
      Logger.getLogger("akka")
    ).foreach(_.setLevel(Level.WARN))

    val csvDirectory  = envVariables.getOrElse("INTERWALLED_CSV_DIRECTORY",  "/mnt/results/benchmark/default.csv")
    val dataDirectory = envVariables.getOrElse("INTERWALLED_DATA_DIRECTORY", "/mnt/data")

    // Use 'Inf' for infinite waiting time
    val timeoutAfter = Duration(envVariables.getOrElse("INTERWALLED_TIMEOUT_AFTER", "30m"))

    MainEnv(
      sparkSession,
      csvDirectory,
      dataDirectory,
      timeoutAfter
    )
  }
}