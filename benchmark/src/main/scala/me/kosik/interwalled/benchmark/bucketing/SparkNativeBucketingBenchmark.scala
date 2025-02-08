package me.kosik.interwalled.benchmark.bucketing

import me.kosik.interwalled.benchmark.utils.{Benchmark, BenchmarkArguments}
import me.kosik.interwalled.spark.join.{IntervalJoin, SparkNativeIntervalJoin}


object SparkNativeBucketingBenchmark extends Benchmark[SparkNativeBucketingBenchmarkArguments] {

  override def prepareJoin(arguments: SparkNativeBucketingBenchmarkArguments): IntervalJoin =
    new SparkNativeIntervalJoin(arguments.bucketSize)
}

case class SparkNativeBucketingBenchmarkArguments(
  bucketSize: Long
) extends BenchmarkArguments
