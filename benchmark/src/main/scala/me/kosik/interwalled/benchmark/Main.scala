package me.kosik.interwalled.benchmark

import me.kosik.interwalled.benchmark.bucketing._
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}


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

  private val testDatas = for {
    testDataSize  <- testDataSizes
    testDataSuite <- testDataSuites
  } yield TestData.fromPath(s"$testDataDirectory/$testDataSuite/$testDataSize", spark)

  private val results = benchmarks flatMap { benchmarkCallback =>
    testDatas map { testData =>
      val benchmarkResult = benchmarkCallback.fn(testData.database, testData.query)

      benchmarkResult match {
        case Success(BenchmarkResult(elapsedMillis)) =>
          s"Benchmark ${benchmarkCallback.description} took $elapsedMillis ms."

        case Failure(reason) =>
          reason.printStackTrace()
          s"Benchmark ${benchmarkCallback.description} failed with ${reason.getMessage}."
      }

    }
  }

  results foreach Console.out.println
}