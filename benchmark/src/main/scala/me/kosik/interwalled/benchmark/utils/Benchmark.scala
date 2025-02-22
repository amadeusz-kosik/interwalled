package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.{TestData, Timer}
import me.kosik.interwalled.domain.test.TestResultRow
import me.kosik.interwalled.spark.join.IntervalJoin

import scala.util.Try


trait Benchmark {

  def prepareBenchmark: BenchmarkCallback = {
    val fn = (testData: TestData) => {
      val timer = Timer.start()

      val result = Try {
        import testData.database.sparkSession.implicits._

        joinImplementation
          .join(testData.database, testData.query)
          .as[TestResultRow]
      }

      val elapsedTime = timer.millisElapsed()


      BenchmarkResult(testData.suite, testData.clustersCount, testData.rowsPerCluster, this.toString, elapsedTime, result)
    }

    BenchmarkCallback(this.toString, fn)
  }

  def joinImplementation: IntervalJoin
}
