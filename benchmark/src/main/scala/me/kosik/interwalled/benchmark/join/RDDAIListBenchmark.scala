package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.RDDAIListIntervalJoin
import me.kosik.interwalled.utility.bucketizer.BucketScale


class RDDAIListBenchmark(bucketScale: Option[Long]) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new RDDAIListIntervalJoin(bucketScale.map(BucketScale))

  override def toString: String = bucketScale match {
    case Some(scale)  => f"bucketed-rdd-ailist-benchmark-$scale"
    case None         => "rdd-ailist-benchmark"
  }
}

