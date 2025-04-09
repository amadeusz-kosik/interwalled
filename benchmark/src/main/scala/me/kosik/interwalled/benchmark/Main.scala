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

  logger.info(f"Running environment: $env.")
  logger.info(f"Running arguments: ${args.mkString("Array(", ", ", ")")}.")

  val Array(useLargeDataset, dataSuite, benchmarkName) = args.take(3)
  val benchmarkArgs = args.drop(3)

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
      new PartitionedAIListBenchmark(benchmarkArgs(0).toInt).prepareBenchmark

    case "spark-native-bucketing" =>
      new SparkNativeBucketingBenchmark(benchmarkArgs(0).toInt).prepareBenchmark
  }

  // --------------------------------------------------------------------

  private implicit val sparkSession: SparkSession = {
    SparkSession.builder()
      .appName(s"InterwalledBenchmark - $benchmarkName")
      .config("spark.driver.memory", env.driverMemory)
      .config("spark.executor.memory", env.executorMemory)
      .master(env.sparkMaster)
      .getOrCreate()
  }

  private val csvWriter: Writer = new PrintWriter(f"./jupyter-lab/data/${benchmark.description}-${dataSuite}.csv")
  csvWriter.write(CSV.header)

  BenchmarkRunner.run(
    benchmark,
    env.dataDirectory,
    testDataSizes,
    dataSuite,
    csvWriter
  )

  csvWriter.close()
}

