package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.{IntervalJoin, SparkNativeIntervalJoin}


class SparkNativeBucketingBenchmark(private val bucketSize: Long) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new SparkNativeIntervalJoin(bucketSize)

  override def toString: String =
    s"spark-native-bucketing-benchmark-$bucketSize"

}
