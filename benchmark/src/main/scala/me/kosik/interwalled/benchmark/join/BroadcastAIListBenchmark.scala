package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.{BroadcastAIListIntervalJoin, IntervalJoin, PartitionedAIListIntervalJoin}


object BroadcastAIListBenchmark extends Benchmark {

  override def joinImplementation: IntervalJoin =
    BroadcastAIListIntervalJoin

  override def toString: String =
    "broadcast-ailist-benchmark"
}

