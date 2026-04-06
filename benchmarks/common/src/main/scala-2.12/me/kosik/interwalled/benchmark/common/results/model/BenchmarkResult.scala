package me.kosik.interwalled.benchmark.common.results.model

import me.kosik.interwalled.benchmark.common.timer.TimerResult


sealed trait BenchmarkResult

case class BenchmarkSuccess(
  timeElapsed: TimerResult,
  resultsRowCount: Long
) extends BenchmarkResult

case class BenchmarkFailure(
  error: Throwable
) extends BenchmarkResult