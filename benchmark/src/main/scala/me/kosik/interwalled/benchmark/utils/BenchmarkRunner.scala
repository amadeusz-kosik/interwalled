package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.TestData
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}


object BenchmarkRunner {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
    sparkFactory: () => SparkSession,
    benchmarks: Seq[BenchmarkCallback],
    testDataDirectory: String,
    testDataSizes: Seq[(Int, Long)],
    testDataSuites: Seq[String]
  ): Seq[BenchmarkResult] = {
    val testDatas = for {
      (clustersCount, rowsCount)  <- testDataSizes
      testDataSuite               <- testDataSuites
    } yield TestData.fromPath(testDataDirectory, testDataSuite, clustersCount, rowsCount) _

    for {
      benchmarkCallback <- benchmarks
      testDataFactory   <- testDatas
      result            <- run(sparkFactory, testDataFactory, benchmarkCallback)
    } yield result
  }

  def run(
    sparkFactory: () => SparkSession,
    testDataFactory: SparkSession => TestData,
    benchmarkCallback: BenchmarkCallback
  ): Option[BenchmarkResult] = Try {
    val spark = sparkFactory()
    val testData = testDataFactory(spark)

    logger.info(s"Running benchmark - ${benchmarkCallback.description} on $testData")
    val result = benchmarkCallback.fn(testData)

    result.result.foreach(_ => ())

    (spark, testData, result)
  } match {
    case Failure(exception) =>
      logger.warn(s"Benchmark ${benchmarkCallback.description} failed flat: ${exception.getMessage.split('\n').head}")
      Option.empty[BenchmarkResult]

    case Success((spark, testData, BenchmarkResult(_, _, _, _, _, Failure(_)))) =>
      logger.warn(s"Benchmark ${benchmarkCallback.description} on $testData failed, stopping SparkContext")
      spark.stop()
      Option.empty[BenchmarkResult]

    case Success((_, _, benchmarkResult: BenchmarkResult)) =>
      Some(benchmarkResult)
  }
}