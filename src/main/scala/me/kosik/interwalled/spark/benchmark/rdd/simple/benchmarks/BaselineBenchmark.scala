
package me.kosik.interwalled.spark.benchmark.rdd.simple.benchmarks

import me.kosik.interwalled.spark.benchmark.rdd.simple.{Benchmark, SimpleInterval}
import org.apache.spark.rdd.RDD

class BaselineBenchmark extends Benchmark {

  override def joinIntervals(left: RDD[SimpleInterval], right: RDD[SimpleInterval]): RDD[(SimpleInterval, SimpleInterval)] =
    left.cartesian(right).filter { case (left, right) => left.from <= right.to && right.from <= left.to }
}
