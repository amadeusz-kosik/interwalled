package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.{IntervalJoin, PartitionedNativeAIListIntervalJoin}

class PartitionedNativeAIListBenchmark(bucketSplit: Long) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new PartitionedNativeAIListIntervalJoin(bucketSplit, 10)

  override def toString: String =
    f"partitioned-native-ailist-benchmark-$bucketSplit"
}

