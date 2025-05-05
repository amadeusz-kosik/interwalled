package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.{TestData, Timer}
import me.kosik.interwalled.domain.test.TestResultRow
import me.kosik.interwalled.spark.join.IntervalJoin

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Try}


trait Benchmark {

  def prepareBenchmark: BenchmarkCallback = {
    val fn = (testData: TestData) => {
      val timer = Timer.start()

      val result = Try {
        import scala.concurrent._
        import scala.concurrent.duration._

        Await.result(Future {
          import testData.database.sparkSession.implicits._

          val resultDataset = joinImplementation
            .join(testData.database, testData.query)
            .as[TestResultRow]

          resultDataset.foreach(_ => ())
        }, 30.minutes) // FIXME
      }

      result match {
        case Failure(e: TimeoutException) =>
          // FIXME: Add logging, message about timeout
          testData.sparkSession.stop()
          throw e

        case _ =>
          ()
      }

      val elapsedTime = timer.millisElapsed()
      BenchmarkResult(testData.suite, testData.clustersCount, testData.rowsPerCluster, this.toString, elapsedTime, result)
    }

    BenchmarkCallback(this.toString, fn)
  }

  def joinImplementation: IntervalJoin
}
