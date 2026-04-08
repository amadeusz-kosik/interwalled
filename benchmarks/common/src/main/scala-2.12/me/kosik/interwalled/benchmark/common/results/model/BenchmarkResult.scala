package me.kosik.interwalled.benchmark.common.results.model

import me.kosik.interwalled.benchmark.common.timer.TimerResult


sealed trait BenchmarkResult[T]

case class BenchmarkSuccess[T](
  timeElapsed:      TimerResult,
  resultRowsCount:  Long,
  result:           T
) extends BenchmarkResult[T]

case class BenchmarkFailure(
  error: Throwable
) extends BenchmarkResult[Any]