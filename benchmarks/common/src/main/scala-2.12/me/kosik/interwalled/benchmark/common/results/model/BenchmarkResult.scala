package me.kosik.interwalled.benchmark.common.results.model

import me.kosik.interwalled.benchmark.common.timer.TimerResult

/* The [T] generic parameter is in fact Dataset / DataFrame from Apache Spark. It is substituted
 *  with generic to evade adding Spark itself as the dependency of this module. This way
 *  Sequila can be loaded with a different version of Spark than the whole project.
 */

sealed trait BenchmarkResult[T]

case class BenchmarkSuccess[T](
  timeElapsed:      TimerResult,
  resultRowsCount:  Long,
  result:           T
) extends BenchmarkResult[T]

case class BenchmarkFailure(
  error: Throwable
) extends BenchmarkResult[Any]