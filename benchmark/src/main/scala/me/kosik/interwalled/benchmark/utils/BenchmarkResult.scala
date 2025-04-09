package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.TimerResult
import scala.util.Try


case class BenchmarkResult(
  dataSuite: String,
  clustersCount: Int,
  rowsPerCluster: Long,
  joinName: String,
  elapsedTime: TimerResult,
  result: Try[Unit]
)
