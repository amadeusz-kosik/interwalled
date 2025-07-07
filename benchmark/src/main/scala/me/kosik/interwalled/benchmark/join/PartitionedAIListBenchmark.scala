package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.RDDAIListIntervalJoin


class PartitionedAIListBenchmark(private val bucketScale: Long) extends Benchmark {

  override def joinImplementation(gatherStatistics: Boolean): IntervalJoin =
    new RDDAIListIntervalJoin(gatherStatistics, bucketScale)

  override def toString: String =
    s"partitioned-ailist-benchmark-$bucketScale"
}

