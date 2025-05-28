package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.{IntervalJoin, PartitionedNativeAIListIntervalJoin, PartitionedNativeAIListIntervalJoinConfig}

class PartitionedNativeAIListBenchmark(bucketSplit: Long, maximumComponentsCount: Int) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new PartitionedNativeAIListIntervalJoin(PartitionedNativeAIListIntervalJoinConfig(
      bucketSplit,
      maximumComponentsCount
    ))

  override def toString: String =
    f"partitioned-native-ailist-benchmark-$bucketSplit-$maximumComponentsCount"
}

