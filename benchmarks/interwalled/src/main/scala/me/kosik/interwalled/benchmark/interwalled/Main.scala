package me.kosik.interwalled.benchmark.interwalled

import me.kosik.interwalled.benchmark.common.env.ApplicationEnv
import me.kosik.interwalled.benchmark.common.results.{BenchmarkOutcomeCSVFormatter, CSVWriter}
import me.kosik.interwalled.benchmark.common.test.data.TestDataSuites
import org.apache.spark.sql.SparkSession


object Main extends App {
  private val applicationEnv =
    ApplicationEnv.buildMain()

  private val sparkSession =
    SparkSession.builder()
      .master(args(0))
      .appName("Interwalled benchmark")
      .getOrCreate()

  private val csvWriter =
    CSVWriter.forPath(BenchmarkOutcomeCSVFormatter)(applicationEnv.csvDirectory)

  Benchmark.runBenchmark(applicationEnv.dataDirectory, TestDataSuites.databioSuites, csvWriter.write)(sparkSession)
}
