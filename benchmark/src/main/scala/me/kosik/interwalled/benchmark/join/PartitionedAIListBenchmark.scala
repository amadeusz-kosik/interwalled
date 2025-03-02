package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.utils.Benchmark
import me.kosik.interwalled.spark.join.{IntervalJoin, PartitionedAIListIntervalJoin}


class PartitionedAIListBenchmark(private val bucketSize: Long) extends Benchmark {

  override def joinImplementation: IntervalJoin =
    new PartitionedAIListIntervalJoin(bucketSize)

  override def toString: String =
    s"partitioned-ailist-benchmark-$bucketSize"
}

