package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.app.MainEnv
import me.kosik.interwalled.benchmark.test.suite.TestDataSuiteReader
import me.kosik.interwalled.benchmark.utils.timer.{Timer, TimerResult}
import me.kosik.interwalled.domain.Interval
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin
import me.kosik.interwalled.utility.stats.model.IntervalJoinRunStats
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object BenchmarkRunner {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def run(request: BenchmarkRequest, env: MainEnv): BenchmarkResult = {
    val database  = TestDataSuiteReader.readDatabase(request.dataSuite, env)
    val query     = TestDataSuiteReader.readQuery(request.dataSuite, env)

    // FIXME
    val benchmarkInputData = {
      import env.sparkSession.implicits._
      IntervalJoin.Input(database.as[Interval], query.as[Interval])
    }

    runBenchmark(benchmarkInputData, request.join, env.timeoutAfter)
      .map { timeElapsed =>
        logger.info("Computation done, running stats gathering.")

        val statistics = gatherStatistics(benchmarkInputData, request.join)
        (timeElapsed, statistics)
      } match {
        case Success((timeElapsed, statistics)) =>
          BenchmarkResult(
            request.dataSuite,
            request.join,
            Success(timeElapsed),
            statistics
          )

        case Failure(exception) =>
          logger.info("Benchmark failed, still running gathering input statistics.")

          BenchmarkResult(
            request.dataSuite,
            request.join,
            Failure[TimerResult](exception),
            None
          )
      }
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