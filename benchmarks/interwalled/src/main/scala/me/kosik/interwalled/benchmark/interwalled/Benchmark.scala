package me.kosik.interwalled.benchmark.interwalled

import me.kosik.interwalled.ailist.model.{AIListConfiguration, Interval}
import me.kosik.interwalled.benchmark.common.results.model.{BenchmarkFailure, BenchmarkOutcome, BenchmarkResult, BenchmarkSuccess}
import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteMetadata
import me.kosik.interwalled.benchmark.common.timer.Timer
import me.kosik.interwalled.benchmark.interwalled.data.TestDataSuiteLoader
import me.kosik.interwalled.spark.join.api.model.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.RDDAIListIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.nio.file.Path


object Benchmark {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def runBenchmark(
    dataDirectory: Path,
    testDataSuites: Array[TestDataSuiteMetadata],
    onBenchmarkCompleted: BenchmarkOutcome[DataFrame] => Unit
  )(implicit sparkSession: SparkSession): Array[BenchmarkOutcome[DataFrame]] = {


    val results = testDataSuites map { testDataSuiteMetadata =>
      logger.info(s"Running test data suite: $testDataSuiteMetadata.")

      val testData = TestDataSuiteLoader.load(dataDirectory, testDataSuiteMetadata)(sparkSession)

      val (database, query) = {
        import sparkSession.implicits._
        (testData.database.as[Interval], testData.query.as[Interval])
      }

      val (timerResult, joinedData) = Timer.timed {
        val joined = {
          val interwalled = new RDDAIListIntervalJoin(RDDAIListIntervalJoin.Config(
            AIListConfiguration.apply,
            PreprocessorConfig.empty
          ))
          interwalled.join(IntervalJoin.Input(database, query))
        }

        // Force Spark to compute the data.
        joined.foreach(_ => ())
        joined
      }

      val joinedDataRowsCount = joinedData.count()

      logger.info(s"Test data suite completed in $timerResult ms.")

      val benchmarkResult: BenchmarkResult[DataFrame] = {
        if(joinedDataRowsCount == testDataSuiteMetadata.expectedOutput)
          BenchmarkSuccess(timerResult, joinedDataRowsCount, joinedData.toDF)
        else
          BenchmarkFailure(timerResult, joinedDataRowsCount, new Exception(s"Expected: ${testDataSuiteMetadata.expectedOutput} rows; Actual: $joinedDataRowsCount rows."))
      }
      val benchmarkOutcome = BenchmarkOutcome("interwalled", testDataSuiteMetadata, benchmarkResult)

      onBenchmarkCompleted(benchmarkOutcome)
      benchmarkOutcome
    }

    results
  }
}
