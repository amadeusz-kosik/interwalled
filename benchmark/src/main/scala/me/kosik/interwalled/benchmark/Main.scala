package me.kosik.interwalled.benchmark

import me.kosik.interwalled.benchmark.join._
import me.kosik.interwalled.benchmark.utils.{BenchmarkCallback, BenchmarkRunner, CSV}
import me.kosik.interwalled.domain.benchmark.ActiveBenchmarks
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.{PrintWriter, Writer}


object Main extends App {

  val logger = LoggerFactory.getLogger(getClass)
  val env = MainEnv.build()
  val Array(useLargeDataset, benchmarkName) = args.take(2)

  logger.info(f"Running environment: $env.")
  logger.info(f"Running arguments: ${args.mkString("Array(", ", ", ")")}.")

  val testDataSizes: Array[(Int, Long)] = {
    if(useLargeDataset.toLowerCase == "true")
      ActiveBenchmarks.TestDataSizes.extended
    else
      ActiveBenchmarks.TestDataSizes.baseline
  }

  private val benchmark: BenchmarkCallback = benchmarkName match {
    case "broadcast-ai-list" =>
      BroadcastAIListBenchmark.prepareBenchmark

    case "partitioned-ai-list" =>
      new PartitionedAIListBenchmark(args(2).toInt).prepareBenchmark

    case "spark-native-bucketing" =>
      new SparkNativeBucketingBenchmark(args(2).toInt).prepareBenchmark
  }

  private val testDataSuites = Array(
    "one-to-all",
    "one-to-one",
    "sparse-16"
  )

  // --------------------------------------------------------------------

  private implicit val sparkSession: SparkSession = {
    SparkSession.builder()
      .appName(s"InterwalledBenchmark - $benchmarkName")
      .config("spark.driver.memory", env.driverMemory)
      .config("spark.executor.memory", env.executorMemory)
      .master(env.sparkMaster)
      .getOrCreate()
  }

  private val csvWriter: Writer = new PrintWriter(f"./jupyter-lab/data/${benchmark.description}.csv")
  csvWriter.write(CSV.header)

  BenchmarkRunner.run(
    benchmark,
    env.dataDirectory,
    testDataSizes,
    testDataSuites,
    csvWriter
  )

  csvWriter.close()
}

