package me.kosik.interwalled.benchmark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import me.kosik.interwalled.benchmark.join.{DriverAIListBenchmark, CachedNativeAIListBenchmark, RDDAIListBenchmark, SparkNativeBenchmark}
import me.kosik.interwalled.benchmark.utils.BenchmarkRunner
import me.kosik.interwalled.main.MainEnv
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.PrintWriter
import scala.concurrent.duration.Duration

class BenchmarkTestSuite extends AnyFunSuite with DataFrameSuiteBase {
  private lazy val env = MainEnv.build()
  private lazy val writer = new PrintWriter(System.out)

  // ---------------------------------------------------------------------------------------------------------------- //

  private lazy val benchmarks = Array(
    DriverAIListBenchmark.prepareBenchmark,

    new CachedNativeAIListBenchmark( 1, None      ).prepareBenchmark,
    new CachedNativeAIListBenchmark( 4, None      ).prepareBenchmark,
    new CachedNativeAIListBenchmark( 8, None      ).prepareBenchmark,
    new CachedNativeAIListBenchmark(16, None      ).prepareBenchmark,
    new CachedNativeAIListBenchmark( 1, Some(1000)).prepareBenchmark,
    new CachedNativeAIListBenchmark( 4, Some(1000)).prepareBenchmark,
    new CachedNativeAIListBenchmark( 8, Some(1000)).prepareBenchmark,
    new CachedNativeAIListBenchmark(16, Some(1000)).prepareBenchmark,

    new RDDAIListBenchmark(None).prepareBenchmark,
    new RDDAIListBenchmark(Some(1000)).prepareBenchmark,

    new SparkNativeBenchmark(None).prepareBenchmark,
    new SparkNativeBenchmark(Some(1000)).prepareBenchmark
  )

  private lazy val dataSuites = Array(
    "all-to-all/10000/1",
    "all-to-one/10000/1",
    "continuous-16/10000/1",
    "one-to-all/10000/1",
    "one-to-one/10000/1",
    "spanning-4/10000/1",
    "spanning-16/10000/1",
    "sparse-16/10000/1"
  )

  for {
    benchmark <- benchmarks
    dataSuite <- dataSuites
  } yield {
    test(s"Example test: ${benchmark.description} on $dataSuite") {
      implicit val sparkSession: SparkSession = spark
      val testData = TestData.load(env.dataDirectory, dataSuite)
      BenchmarkRunner.run(testData, benchmark, writer, Duration.Inf)
    }
  }
}
