package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.RDDAIListIntervalJoin
import me.kosik.interwalled.utility.bucketizer.BucketScale


class RDDAIListBenchmark(bucketScale: Long) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new RDDAIListIntervalJoin(Some(BucketScale(bucketScale)))

  override def toString: String =
    s"bucketed-rdd-ailist-benchmark-$bucketScale"
}

