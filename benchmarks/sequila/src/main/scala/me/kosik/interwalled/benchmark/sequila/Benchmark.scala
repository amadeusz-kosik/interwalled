package me.kosik.interwalled.benchmark.sequila

import me.kosik.interwalled.benchmark.common.results.model.{BenchmarkOutcome, BenchmarkSuccess}
import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteMetadata
import me.kosik.interwalled.benchmark.common.timer.Timer
import me.kosik.interwalled.benchmark.sequila.data.TestDataSuiteLoader
import org.apache.spark.sql.{DataFrame, SequilaSession, SparkSession}
import org.slf4j.LoggerFactory

import java.nio.file.Path


object Benchmark {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def runBenchmark(
    dataDirectory: Path,
    testDataSuites: Array[TestDataSuiteMetadata],
    onBenchmarkCompleted: BenchmarkOutcome[DataFrame] => Unit
  )(implicit sparkSession: SparkSession): Array[BenchmarkOutcome[DataFrame]] = {
    SequilaSession(sparkSession)

    val results = testDataSuites map { testDataSuiteMetadata =>
      logger.info(s"Running test data suite: $testDataSuiteMetadata.")

      val testData = TestDataSuiteLoader.load(dataDirectory, testDataSuiteMetadata)(sparkSession)

      val database = testData.database.alias("database")
      val query    = testData.query.alias("query")

      val (timerResult, joinedData) = Timer.timed {
        val joined = database.join(query,
          (database.col("from") <= query.col("to")) &&
            (database.col("to")   >= query.col("from")) &&
            (database.col("key") === query.col("key"))
        )

        // Force Spark to compute the data.
        joined.foreach(_ => ())
        joined
      }

      val joinedDataRowsCount = joinedData.count()

      logger.info(s"Test data suite completed in $timerResult ms.")

      val benchmarkOutcome = BenchmarkOutcome("sequila", testDataSuiteMetadata, BenchmarkSuccess(timerResult, joinedDataRowsCount, joinedData))
      onBenchmarkCompleted(benchmarkOutcome)

      benchmarkOutcome
    }

    results
  }
}
