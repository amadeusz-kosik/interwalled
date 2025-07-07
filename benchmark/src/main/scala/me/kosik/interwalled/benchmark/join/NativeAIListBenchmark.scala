package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.NativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.api.IntervalJoin

object NativeAIListBenchmark extends Benchmark {

  override def joinImplementation(gatherStatistics: Boolean): IntervalJoin =
    new NativeAIListIntervalJoin(gatherStatistics)

  override def toString: String =
    "native-ailist-benchmark-v2"
}

