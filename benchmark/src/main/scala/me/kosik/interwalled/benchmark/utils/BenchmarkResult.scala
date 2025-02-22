package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.TimerResult
import me.kosik.interwalled.domain.test.TestResultRow
import org.apache.spark.sql.Dataset

import scala.util.Try


case class BenchmarkResult(
  dataSuite: String,
  clustersCount: Int,
  rowsPerCluster: Long,
  joinName: String,
  elapsedTime: TimerResult,
  result: Try[Dataset[TestResultRow]]
)
