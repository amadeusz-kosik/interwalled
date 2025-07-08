package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.NativeAIListIntervalJoin

class NativeAIListBenchmark(maximumComponentsCount: Int) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new NativeAIListIntervalJoin(AIListConfig(maximumComponentsCount = maximumComponentsCount), None)

  override def toString: String =
    f"native-ailist-benchmark-$maximumComponentsCount"
}

