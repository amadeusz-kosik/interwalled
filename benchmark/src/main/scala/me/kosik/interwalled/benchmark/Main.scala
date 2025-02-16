package me.kosik.interwalled.benchmark

import me.kosik.interwalled.benchmark.join._
import me.kosik.interwalled.benchmark.utils.{BenchmarkCallback, BenchmarkRunner, CSV}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}


object Main extends App {

  private val Array(sparkMaster, driverMemory, testDataDirectory) = args.take(3)

  private val testDataSizes = Array(
       100L,
      1000L,
     10000L,
    100000L
  )

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
    "one-to-many",
    "one-to-one"
  )

  // --------------------------------------------------------------------

  private val spark: SparkSession = SparkSession.builder()
    .appName("InterwalledBenchmark")
    .config("spark.driver.memory", driverMemory)
    .config("spark.executor.memory", driverMemory)
    .master(sparkMaster)
    .getOrCreate()

  private val results = BenchmarkRunner.run(
    spark,
    benchmarks,
    testDataDirectory,
    testDataSizes,
    testDataSuites
  )

  System.out.println("")
  System.out.println("CSV results (success only):")

  CSV
    .toCSV(results.flatMap {
      case Success(benchmarkResult) =>
        Some(benchmarkResult)

      case Failure(_) =>
        None
    })
    .foreach(System.out.println)

  System.out.println("")
  System.out.println("Benchmark summary (human-friendly):")

  results
    .map {
      case Success(benchmarkResult) =>
        s"Benchmark ${benchmarkResult.joinName} " +
          s"on ${benchmarkResult.dataSuite} (${benchmarkResult.dataSize} rows) " +
          s"took: \n\t ${benchmarkResult.elapsedTime} ms."

      case Failure(failureReason) =>
        failureReason.printStackTrace()
        s"Benchmark failed with ${failureReason.getMessage}."
    }
    .foreach(Console.out.println)
}

