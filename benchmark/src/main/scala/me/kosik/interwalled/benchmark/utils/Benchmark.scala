package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.{TestData, Timer}
import me.kosik.interwalled.spark.join.IntervalJoin

import scala.util.Try


trait Benchmark[T <: BenchmarkArguments] {

  def prepareBenchmark(arguments: T): BenchmarkCallback = {
    val benchmarkDescription = arguments.toString
    val fn = (testData: TestData) => Try {
      val joinImplementation = prepareJoin(arguments)
      val timer = Timer.start()

      val result = joinImplementation.join(testData.database, testData.query)
      result.foreach(_ => ())

      val msElapsed = timer.millisElapsed()
      BenchmarkResult(testData.description, benchmarkDescription, msElapsed)
    }

    BenchmarkCallback(benchmarkDescription, fn)
  }

  def prepareJoin(arguments: T): IntervalJoin
}

trait BenchmarkArguments
