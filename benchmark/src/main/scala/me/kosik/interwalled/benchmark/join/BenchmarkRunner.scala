package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.ailist.model.Interval
import me.kosik.interwalled.benchmark.app.ApplicationEnv
import me.kosik.interwalled.benchmark.common.results.model.{BenchmarkFailure, BenchmarkOutcome, BenchmarkSuccess}
import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteLoader
import me.kosik.interwalled.benchmark.common.timer.{Timer, TimerResult}
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin
import me.kosik.interwalled.utility.stats.model.IntervalJoinRunStats
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object BenchmarkRunner {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def run(request: BenchmarkRequest, env: ApplicationEnv): BenchmarkOutcome = {
    val testData = TestDataSuiteLoader.load(env.dataDirectory, request.dataSuite)(env.sparkSession)

    // FIXME
    val benchmarkInputData = {
      import env.sparkSession.implicits._
      IntervalJoin.Input(testData.database.as[Interval], testData.query.as[Interval])
    }

    val benchmarkResult = runBenchmark(benchmarkInputData, request.join, env.timeoutAfter)
      .map { timeElapsed =>
        logger.info("Computation done, running stats gathering.")

        val statistics = gatherStatistics(benchmarkInputData, request.join)
        (timeElapsed, statistics)
      } match {
        case Success((timeElapsed, statistics)) => // FIXME get
          BenchmarkSuccess(timeElapsed, statistics.get.resultRowsCount)

        case Failure(exception) =>
          logger.info("Benchmark failed, still running gathering input statistics.")
          BenchmarkFailure(exception)
      }

    BenchmarkOutcome(request.join.toString, request.dataSuite.suite, benchmarkResult)
  }

  private def runBenchmark(inputData: IntervalJoin.Input, join: IntervalJoin, timeout: Duration): Try[TimerResult] = {
    implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    val benchmarkTimer = Timer.start()

    val resultPromise = Future {
      // Force Spark to compute the data.
      // Discard the result to not pollute benchmarks with output I/O.
      // Interrupt if job takes too long to complete.

      logger.info(s"Running benchmark with timeout of $timeout.")
      val resultDataset = join.join(inputData).data

      logger.info("Forcing Apache Spark to materialize the DAG.")
      resultDataset.foreach(_ => ())
    }

    Try(Await.result(resultPromise, timeout))
      .map(_ => benchmarkTimer.millisElapsed())
  }

  private def gatherStatistics(inputData: IntervalJoin.Input, join: IntervalJoin): Option[IntervalJoinRunStats] = {
    Try(join.join(inputData, gatherStatistics = true))
      .toOption
      .flatMap(_.statistics)
  }
}