package me.kosik.interwalled.benchmark.join.algorithms

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.DriverAIListIntervalJoin


object DriverAIListBenchmark extends Benchmark {

  override def joinImplementation: IntervalJoin =
    DriverAIListIntervalJoin

  override def toString: String =
    "driver-ailist-benchmark"
}

