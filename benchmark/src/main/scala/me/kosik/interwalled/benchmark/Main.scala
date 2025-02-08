package me.kosik.interwalled.benchmark

import me.kosik.interwalled.benchmark.bucketing._
import me.kosik.interwalled.benchmark.utils.{BenchmarkCallback, BenchmarkRunner}
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
    val partitionedAIList = for {
      bucketSize <- bucketsSize
      arguments   = PartitionedAIListBenchmarkArguments(bucketSize)
    } yield PartitionedAIListBenchmark.prepareBenchmark(arguments)

    val sparkNativeBucketing = for {
      bucketSize <- bucketsSize
      arguments   = SparkNativeBucketingBenchmarkArguments(bucketSize)
    } yield SparkNativeBucketingBenchmark.prepareBenchmark(arguments)

    partitionedAIList ++ sparkNativeBucketing
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
    .master(sparkMaster)
    .getOrCreate()

  private val results = BenchmarkRunner.run(
    spark,
    benchmarks,
    testDataDirectory,
    testDataSizes,
    testDataSuites
  )

  results
    .map {
      case Success(benchmarkResult) =>
        s"Benchmark ${benchmarkResult.benchmarkDescription} " +
          s"on ${benchmarkResult.dataDescription} " +
          s"took ${benchmarkResult.elapsedTime} ms."

      case Failure(failureReason) =>
        failureReason.printStackTrace()
        s"Benchmark failed with ${failureReason.getMessage}."
    }
    .foreach(Console.out.println)
}

