package me.kosik.interwalled.benchmark

import me.kosik.interwalled.domain.Interval
import org.apache.spark.sql.{Dataset, SparkSession}

trait Benchmark {

  def run(
    database: Dataset[Interval[String]],
    query: Dataset[Interval[String]],
    spark: SparkSession,
    extraArguments: Array[String]
  ): Unit

}
