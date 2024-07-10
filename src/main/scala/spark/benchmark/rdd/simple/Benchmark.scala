package com.eternalsh.interwalled
package spark.benchmark.rdd.simple

import org.apache.spark.rdd.RDD

trait Benchmark {

  def joinIntervals(left: RDD[SimpleInterval], right: RDD[SimpleInterval]): RDD[(SimpleInterval, SimpleInterval)]
}
