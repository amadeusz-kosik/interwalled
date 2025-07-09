package me.kosik.interwalled.benchmark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import me.kosik.interwalled.benchmark.join.{DriverAIListBenchmark, NativeAIListBenchmark, RDDAIListBenchmark, SparkNativeBenchmark}
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

    new NativeAIListBenchmark(1000, None).prepareBenchmark,
    new NativeAIListBenchmark(1000, Some(1000)).prepareBenchmark,

    new RDDAIListBenchmark(None).prepareBenchmark,
    new RDDAIListBenchmark(Some(1000)).prepareBenchmark,

    new SparkNativeBenchmark(None).prepareBenchmark,
    new SparkNativeBenchmark(Some(1000)).prepareBenchmark
  )


  for {
    benchmark <- benchmarks
  } yield {
    test(s"Example test: ${benchmark.description}") {
      implicit val sparkSession: SparkSession = spark
      val testData = TestData.load(env.dataDirectory, "one-to-one/10000/1")
      BenchmarkRunner.run(testData, benchmark, writer, Duration.Inf)
    }
  }
}
