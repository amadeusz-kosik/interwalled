package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.common.test.data.TestDataSuite
import me.kosik.interwalled.benchmark.utils.timer.TimerResult
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.utility.stats.model.IntervalJoinRunStats

import scala.util.Try


case class BenchmarkResult(
  dataSuite: TestDataSuite,
  join: IntervalJoin,
  result: Try[TimerResult],
  statistics: Option[IntervalJoinRunStats]
)