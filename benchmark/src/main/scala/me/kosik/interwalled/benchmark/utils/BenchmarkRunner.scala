package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.TestDataBuilder
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.Writer
import scala.util.Try


object BenchmarkRunner {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
    benchmarkCallback: BenchmarkCallback,
    testDataDirectory: String,
    testDataSizes: Seq[(Int, Long)],
    testDataSuites: Seq[String],
    outputWriter: Writer
  )(implicit sparkSession: SparkSession): Unit = {

    val testDataBuilders = for {
      (clustersCount, rowsCount)  <- testDataSizes
      testDataSuite               <- testDataSuites
    } yield TestDataBuilder(testDataDirectory, testDataSuite, clustersCount, rowsCount)

    testDataBuilders foreach { testDataBuilder =>
      run(testDataBuilder, benchmarkCallback, outputWriter)
    }
  }

  def run(
    testDatabuilder: TestDataBuilder,
    benchmarkCallback: BenchmarkCallback,
    outputWriter: Writer
  )(implicit sparkSession: SparkSession): Unit = {

    val appName = f"${benchmarkCallback.description} on $testDatabuilder"
    val testData = testDatabuilder(sparkSession)

    logger.info(s"Running benchmark - $appName")
    val result = benchmarkCallback.fn(testData)
    result.result.foreach(_ => ())

    outputWriter.write(CSV.row(result))
    outputWriter.flush()
  }
}