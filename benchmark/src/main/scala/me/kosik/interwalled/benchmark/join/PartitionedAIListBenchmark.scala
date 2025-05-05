package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.{IntervalJoin, PartitionedAIListIntervalJoin}


class PartitionedAIListBenchmark(private val bucketScale: Long) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new PartitionedAIListIntervalJoin(bucketScale)

  override def toString: String =
    s"partitioned-ailist-benchmark-$bucketScale"
}

