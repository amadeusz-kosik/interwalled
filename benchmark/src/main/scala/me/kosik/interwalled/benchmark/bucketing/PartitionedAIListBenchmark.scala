package me.kosik.interwalled.benchmark.bucketing

import me.kosik.interwalled.benchmark.utils.{Benchmark, BenchmarkArguments}
import me.kosik.interwalled.spark.join.{IntervalJoin, PartitionedAIListIntervalJoin}


object PartitionedAIListBenchmark extends Benchmark[PartitionedAIListBenchmarkArguments] {

  override def prepareJoin(arguments: PartitionedAIListBenchmarkArguments): IntervalJoin =
    new PartitionedAIListIntervalJoin(arguments.bucketSize)
}

case class PartitionedAIListBenchmarkArguments(
  bucketSize: Long
) extends BenchmarkArguments
