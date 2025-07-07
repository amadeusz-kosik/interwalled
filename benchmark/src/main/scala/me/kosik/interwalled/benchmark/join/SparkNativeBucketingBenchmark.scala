package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.SparkNativeIntervalJoin


class SparkNativeBucketingBenchmark(private val bucketSize: Long) extends Benchmark {

  override def joinImplementation(gatherStatistics: Boolean): IntervalJoin =
    new SparkNativeIntervalJoin(gatherStatistics, bucketSize)

  override def toString: String =
    s"spark-native-bucketing-benchmark-$bucketSize"

}
