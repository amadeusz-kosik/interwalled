package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.{CachedNativeAIListIntervalJoin, NativeAIListIntervalJoin}
import me.kosik.interwalled.utility.bucketizer.BucketScale

class CachedNativeAIListBenchmark(maximumComponentsCount: Int, bucketScale: Option[Long]) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new CachedNativeAIListIntervalJoin(AIListConfig(maximumComponentsCount = maximumComponentsCount), bucketScale.map(BucketScale))

  override def toString: String = bucketScale match {
    case Some(scale)  => f"bucketed-cached-native-ailist-benchmark-$maximumComponentsCount-$scale"
    case None         => f"cached-native-ailist-benchmark-$maximumComponentsCount"
  }
}

