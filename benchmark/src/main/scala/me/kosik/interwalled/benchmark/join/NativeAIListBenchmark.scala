package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.NativeAIListIntervalJoin
import me.kosik.interwalled.utility.bucketizer.BucketScale

class NativeAIListBenchmark(maximumComponentsCount: Int, bucketScale: Option[Long]) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new NativeAIListIntervalJoin(AIListConfig(maximumComponentsCount = maximumComponentsCount), bucketScale.map(BucketScale))

  override def toString: String =
    f"native-ailist-benchmark-$maximumComponentsCount-$bucketScale"
}

