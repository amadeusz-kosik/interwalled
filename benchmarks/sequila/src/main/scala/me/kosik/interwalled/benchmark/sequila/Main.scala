package me.kosik.interwalled.benchmark.sequila

import me.kosik.interwalled.benchmark.common.test.data.TestDataSuites
import org.apache.spark.sql.SparkSession


object Main extends App {
  val sparkSession = SparkSession.builder().appName("Sequila benchmark").getOrCreate()
  Benchmark.runBenchmark("FIXME", TestDataSuites.databioSuites)(sparkSession)
}
