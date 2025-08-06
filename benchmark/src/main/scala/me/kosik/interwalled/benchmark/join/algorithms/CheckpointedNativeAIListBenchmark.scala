package me.kosik.interwalled.benchmark.join.algorithms

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.CheckpointedNativeAIListIntervalJoin
import me.kosik.interwalled.utility.bucketizer.BucketScale

class CheckpointedNativeAIListBenchmark(maximumComponentsCount: Int, bucketScale: Option[Long]) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new CheckpointedNativeAIListIntervalJoin("temporary/CheckpointedNativeAIListBenchmark/", AIListConfig(maximumComponentsCount = maximumComponentsCount), bucketScale.map(BucketScale))

  override def toString: String = bucketScale match {
    case Some(scale)  => f"bucketed-checkpointed-native-ailist-benchmark-$maximumComponentsCount-$scale"
    case None         => f"checkpointed-native-ailist-benchmark-$maximumComponentsCount"
  }
}

