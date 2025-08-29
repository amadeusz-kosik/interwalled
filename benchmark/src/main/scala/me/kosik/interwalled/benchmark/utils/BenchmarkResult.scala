package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.test.suite.TestDataSuite
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalStatistics

import scala.util.Try


case class BenchmarkResult(
  dataSuite: TestDataSuite,
  join: IntervalJoin,
  result: Try[TimerResult],
  statistics: Option[IntervalStatistics]
)