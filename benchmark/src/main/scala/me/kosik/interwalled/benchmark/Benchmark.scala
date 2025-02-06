package me.kosik.interwalled.benchmark

import me.kosik.interwalled.domain.Interval
import me.kosik.interwalled.spark.join.IntervalJoin
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

trait Benchmark[T <: BenchmarkArguments] {

  def prepareBenchmark(arguments: T): BenchmarkCallback = {
    val description = arguments.toString
    val fn = (database: Dataset[Interval[String]], query: Dataset[Interval[String]]) => Try {
      val joinImplementation = prepareJoin(arguments)
      val timer = Timer.start()

      val result = joinImplementation.join(database, query)
      result.foreach(_ => ())

      val msElapsed = timer.millisElapsed()
      BenchmarkResult(msElapsed)
    }

    BenchmarkCallback(description, fn)
  }

  def prepareJoin(arguments: T): IntervalJoin
}

trait BenchmarkArguments

case class BenchmarkCallback(
  description: String,
  fn: (Dataset[Interval[String]], Dataset[Interval[String]]) => Try[BenchmarkResult]
)

case class BenchmarkResult(elapsedTime: Long)