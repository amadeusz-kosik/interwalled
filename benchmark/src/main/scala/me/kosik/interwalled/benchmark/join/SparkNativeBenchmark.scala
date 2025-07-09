package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.{NativeAIListIntervalJoin, SparkNativeIntervalJoin}
import me.kosik.interwalled.utility.bucketizer.BucketScale

class SparkNativeBenchmark(bucketScale: Option[Long]) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new SparkNativeIntervalJoin(bucketScale.map(BucketScale))

  override def toString: String =
    f"spark-native-benchmark-$bucketScale"
}

