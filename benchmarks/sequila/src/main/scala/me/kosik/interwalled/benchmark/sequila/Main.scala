package me.kosik.interwalled.benchmark.sequila

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
      .appName("Sequila benchmark")
      .getOrCreate()

  private val csvWriter =
    CSVWriter.forPath(BenchmarkOutcomeCSVFormatter)(applicationEnv.csvDirectory)

  // TODO: Skip 6
  Benchmark.runBenchmark(applicationEnv.dataDirectory, TestDataSuites.databioSuites, csvWriter.write)(sparkSession)
}
