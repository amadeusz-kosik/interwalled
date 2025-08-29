package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.app.MainEnv
import me.kosik.interwalled.benchmark.test.suite.TestDataSuiteReader
import me.kosik.interwalled.domain.Interval
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.{IntervalJoin, IntervalStatistics}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object BenchmarkRunner {

  def run(request: BenchmarkRequest, env: MainEnv): BenchmarkResult = {
    val database  = TestDataSuiteReader.readDatabase(request.dataSuite, env)
    val query     = TestDataSuiteReader.readQuery(request.dataSuite, env)

    // FIXME
    val benchmarkInputData = {
      import env.sparkSession.implicits._
      IntervalJoin.Input(database.as[Interval[String]], query.as[Interval[String]])
    }

    runBenchmark(benchmarkInputData, request.join, env.timeoutAfter)
      .map { timeElapsed =>
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
          BenchmarkResult(
            request.dataSuite,
            request.join,
            Failure[TimerResult](exception),
            None
          )
      }
  }

  private def runBenchmark(inputData: IntervalJoin.Input[String], join: IntervalJoin, timeout: Duration): Try[TimerResult] = {
    implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    val benchmarkTimer = Timer.start()

    val resultPromise = Future {
      // Force Spark to compute the data.
      // Discard the result to not pollute benchmarks with output I/O.
      // Interrupt if job takes too long to complete.

      val resultDataset = join.join(inputData).data
      resultDataset.foreach(_ => ())
    }

    Try(Await.result(resultPromise, timeout))
      .map(_ => benchmarkTimer.millisElapsed())
  }

  private def gatherStatistics(inputData: IntervalJoin.Input[String], join: IntervalJoin): Option[IntervalStatistics] = {
    Try(join.join(inputData, gatherStatistics = true))
      .toOption
      .flatMap(_.statistics)
  }
}