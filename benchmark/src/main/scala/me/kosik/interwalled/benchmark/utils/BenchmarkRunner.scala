package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.TestData
import org.apache.spark.sql.SparkSession

import scala.util.Try


object BenchmarkRunner {

  def run(
    spark: SparkSession,
    benchmarks: Seq[BenchmarkCallback],
    testDataDirectory: String,
    testDataSizes: Seq[Long],
    testDataSuites: Seq[String]
  ): Seq[Try[BenchmarkResult]] = {
    val testDatas = for {
      testDataSize  <- testDataSizes
      testDataSuite <- testDataSuites
    } yield TestData.fromPath(testDataDirectory, testDataSuite, testDataSize, spark)

    // Warmup to fix first benchmark taking too long.
    testDatas.head.database.foreach(_ => ())
    testDatas.head.query.foreach(_ => ())

    for {
      benchmarkCallback <- benchmarks
      testData          <- testDatas
    } yield benchmarkCallback.fn(testData)
  }
}