package me.kosik.interwalled.benchmark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import me.kosik.interwalled.benchmark.join.BucketedRDDAIListBenchmark
import me.kosik.interwalled.benchmark.utils.BenchmarkRunner
import me.kosik.interwalled.main.MainEnv
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.PrintWriter
import scala.concurrent.duration.Duration

class BucketedRDDAIListBenchmarkTestSuite extends AnyFunSuite with DataFrameSuiteBase {

  // ---------------------------------------------------------------------------------------------------------------- //

  test("example test") {
    implicit val sparkSession: SparkSession = spark

    val env = MainEnv.build()
    val benchmark = new BucketedRDDAIListBenchmark(1000).prepareBenchmark
    val writer = new PrintWriter(System.out)

    BenchmarkRunner.run(TestData.load(env.dataDirectory, "one-to-one/10000/1"), benchmark, writer, Duration.Inf)
  }
}
