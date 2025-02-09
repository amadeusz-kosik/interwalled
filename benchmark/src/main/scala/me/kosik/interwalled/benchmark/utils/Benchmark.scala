package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.{TestData, Timer}
import me.kosik.interwalled.spark.join.IntervalJoin

import scala.util.Try


trait Benchmark {

  def prepareBenchmark: BenchmarkCallback = {
    val fn = (testData: TestData) => Try {
      val timer = Timer.start()

      val result = joinImplementation.join(testData.database, testData.query)
      result.foreach(_ => ())

      val msElapsed = timer.millisElapsed()
      BenchmarkResult(testData.suite, testData.size, this.toString, msElapsed)
    }

    BenchmarkCallback(this.toString, fn)
  }

  def joinImplementation: IntervalJoin
}
