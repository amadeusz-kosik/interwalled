package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.{IntervalJoin, NativeAIListIntervalJoin}

object NativeAIListBenchmark extends Benchmark {

  override def joinImplementation: IntervalJoin =
    NativeAIListIntervalJoin

  override def toString: String =
    "native-ailist-benchmark-v2"
}

