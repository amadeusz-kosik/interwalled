package me.kosik.interwalled.benchmark

import me.kosik.interwalled.benchmark.join._
import me.kosik.interwalled.benchmark.utils.{BenchmarkCallback, BenchmarkRunner, CSV}
import me.kosik.interwalled.domain.benchmark.ActiveBenchmarks
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory



object Main extends App {

  val logger = LoggerFactory.getLogger(getClass)
  val env = MainEnv.build()
  val Array(useLargeDataset) = args.take(1)

  val testDataSizes: Array[(Int, Long)] = {
    if(useLargeDataset.toLowerCase == "true")
      ActiveBenchmarks.TestDataSizes.extended
    else
      ActiveBenchmarks.TestDataSizes.baseline
  }

  private val bucketsSize = Array(
      10,
     100,
    1000,
  )

  private val benchmarks: Seq[BenchmarkCallback] = {
    val broadcastAIList = Array(BroadcastAIListBenchmark)

    val partitionedAIList = for {
      bucketSize <- bucketsSize
    } yield new PartitionedAIListBenchmark(bucketSize)

    val sparkNativeBucketing = for {
      bucketSize <- bucketsSize
    } yield new SparkNativeBucketingBenchmark(bucketSize)

    (sparkNativeBucketing ++ broadcastAIList ++ partitionedAIList).map(_.prepareBenchmark)
  }

  private val testDataSuites = Array(
    "one-to-all",
    "one-to-one",
    "sparse-16"
  )

  // --------------------------------------------------------------------

  private def sparkFactory(): SparkSession = {
    SparkSession.getActiveSession.foreach(_.stop())

    SparkSession.builder()
      .appName("InterwalledBenchmark")
      .config("spark.driver.memory", env.driverMemory)
      .config("spark.executor.memory", env.executorMemory)
      .master(env.sparkMaster)
      .getOrCreate()
  }

  private val results = BenchmarkRunner.run(
    sparkFactory,
    benchmarks,
    env.dataDirectory,
    testDataSizes,
    testDataSuites
  )

  System.out.println("")
  System.out.println("CSV results:")

  CSV
    .toCSV(results)
    .foreach(System.out.println)
}

