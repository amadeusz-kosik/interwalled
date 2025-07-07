package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.DriverAIListIntervalJoin


object BroadcastAIListBenchmark extends Benchmark {

  override def joinImplementation(gatherStatistics: Boolean): IntervalJoin =
    new DriverAIListIntervalJoin(gatherStatistics)

  override def toString: String =
    "broadcast-ailist-benchmark"
}

