package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.{TestData, Timer}
import me.kosik.interwalled.domain.test.TestResultRow
import me.kosik.interwalled.spark.join.IntervalJoin

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Try}


trait Benchmark {

  def prepareBenchmark: BenchmarkCallback = {
    val fn = (testData: TestData) => {
      import testData.database.sparkSession.implicits._

      joinImplementation
        .join(testData.database, testData.query)
        .as[TestResultRow]
    }

    BenchmarkCallback(this.toString, fn)
  }

  def joinImplementation: IntervalJoin
}
