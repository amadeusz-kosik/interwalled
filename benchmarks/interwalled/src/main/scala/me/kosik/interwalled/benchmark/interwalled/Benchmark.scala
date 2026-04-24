package me.kosik.interwalled.benchmark.interwalled

import me.kosik.interwalled.ailist.model.{AIListConfiguration, Interval}
import me.kosik.interwalled.benchmark.common.results.model.{BenchmarkFailure, BenchmarkOutcome, BenchmarkResult, BenchmarkSuccess}
import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteMetadata
import me.kosik.interwalled.benchmark.common.timer.Timer
import me.kosik.interwalled.benchmark.interwalled.data.TestDataSuiteLoader
import me.kosik.interwalled.spark.join.api.model.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.RDDAIListIntervalJoin
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.nio.file.Path
import scala.util.{Failure, Success, Try}


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
        (
          testData.database.as[Interval],
          testData.query.as[Interval]
        )
      }

//      val jobResult = Try { Timer.timed {
//        val joined = {
//          val interwalled = new RDDAIListIntervalJoin(AIListConfiguration.apply)
//          interwalled.join(IntervalJoin.Input(database, query))
//        }
//
//        // Force Spark to compute the data.
//        joined.foreach(_ => ())
//        joined
//      }}
//
//      val benchmarkResult: BenchmarkResult[DataFrame] =  jobResult match {
//        case Success((timerResult, joinedData)) =>
//          val joinedDataRowsCount = joinedData.count()
//
//          if(joinedDataRowsCount == testDataSuiteMetadata.expectedOutput)
//            BenchmarkSuccess(timerResult, joinedDataRowsCount, joinedData.toDF)
//          else
//            BenchmarkFailure(timerResult, joinedDataRowsCount, new Exception(s"Expected: ${testDataSuiteMetadata.expectedOutput} rows; Actual: $joinedDataRowsCount rows."))
//
//        case Failure(exception) =>
//          BenchmarkFailure(exception)
//      }

      Thread.sleep(Long.MaxValue)
      val benchmarkOutcome = BenchmarkOutcome("interwalled", testDataSuiteMetadata, benchmarkResult)

      onBenchmarkCompleted(benchmarkOutcome)
      benchmarkOutcome
    }

    results
  }
}
