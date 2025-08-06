package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.spark.join.api.model.IntervalStatistics

import scala.util.Try


case class BenchmarkResult(
  dataSuite: String,
  joinName: String,
  result: Try[TimerResult],
  statistics: Option[IntervalStatistics]
)
