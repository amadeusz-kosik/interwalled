package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.TimerResult
import me.kosik.interwalled.spark.join.api.model.IntervalStatistics

import scala.util.Try


case class BenchmarkResult(
  dataSuite: String,
  joinName: String,
  elapsedTime: TimerResult,
  result: Try[Unit],
  statistics: Option[IntervalStatistics]
)
