package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.{TestData, Timer}
import me.kosik.interwalled.domain.test.TestResultRow
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Try}


trait Benchmark {

  def prepareBenchmark: BenchmarkCallback = {
    val fn = (testData: TestData) => {
      import testData.database.sparkSession.implicits._

      joinImplementation(false)
        .join(Input(testData.database, testData.query))
        .data.as[TestResultRow]
    }

    val statistics = (testData: TestData) => {
      import testData.database.sparkSession.implicits._

      joinImplementation(true)
        .join(Input(testData.database, testData.query))
        .statistics
    }

    BenchmarkCallback(this.toString, fn, statistics)
  }

  def joinImplementation(gatherStatistics: Boolean): IntervalJoin
}
