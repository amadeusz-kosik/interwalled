package me.kosik.interwalled.benchmark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import me.kosik.interwalled.benchmark.utils.csv.CSVWriter
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.PrintWriter
import scala.concurrent.duration.Duration


class BenchmarkTestSuite extends AnyFunSuite with DataFrameSuiteBase {
//  private lazy val env = MainEnv.build()
//  private lazy val writer = CSVWriter.open(new PrintWriter(System.out))
//
//  // ---------------------------------------------------------------------------------------------------------------- //
//
//  private lazy val benchmarks = Array(
//    DriverAIListBenchmark.prepareBenchmark,
//
//    new CachedNativeAIListBenchmark( 1, None      ).prepareBenchmark,
//    new CachedNativeAIListBenchmark( 4, None      ).prepareBenchmark,
//    new CachedNativeAIListBenchmark( 8, None      ).prepareBenchmark,
//    new CachedNativeAIListBenchmark(16, None      ).prepareBenchmark,
//    new CachedNativeAIListBenchmark( 1, Some(1000)).prepareBenchmark,
//    new CachedNativeAIListBenchmark( 4, Some(1000)).prepareBenchmark,
//    new CachedNativeAIListBenchmark( 8, Some(1000)).prepareBenchmark,
//    new CachedNativeAIListBenchmark(16, Some(1000)).prepareBenchmark,
//
//    new RDDAIListBenchmark(None).prepareBenchmark,
//    new RDDAIListBenchmark(Some(1000)).prepareBenchmark,
//
//    new SparkNativeBenchmark(None).prepareBenchmark,
//    new SparkNativeBenchmark(Some(1000)).prepareBenchmark
//  )
//
//  private lazy val edgeDataSuites = Array(
//    "edge/all-to-all/10000/1",
//    "edge/all-to-one/10000/1",
//    "edge/continuous-16/10000/1",
//    "edge/one-to-all/10000/1",
//    "edge/one-to-one/10000/1",
//    "edge/spanning-4/10000/1",
//    "edge/spanning-16/10000/1",
//    "edge/sparse-16/10000/1"
//  )
//
//  private lazy val databioDataSuites = Array(
//    // S-Size
//    ("S-SIZE-(1-2)", "databio-8p/fBrain-DS14718", "databio-8p/exons"),
//    ("S-SIZE-(2-7)", "databio-8p/fBrain-DS14718", "databio-8p/ex-anno")
//  )
//
//  for {
//    benchmark <- benchmarks
//    dataSuite <- edgeDataSuites
//    databaseDatasetPath = f"${env.dataDirectory}/$dataSuite/database.parquet"
//    queryDatasetPath    = f"${env.dataDirectory}/$dataSuite/query.parquet"
//  } yield {
//    test(s"Example test, edge case: ${benchmark.description} on $dataSuite") {
//      implicit val sparkSession: SparkSession = spark
//      val testData = TestDataLoader.load(dataSuite, databaseDatasetPath, queryDatasetPath)
//      BenchmarkRunner.run(testData, benchmark, writer, Duration.Inf)
//    }
//  }
//
//  for {
//    benchmark <- benchmarks
//    (dataSuite, databaseDataset, queryDataset) <- databioDataSuites
//    databaseDatasetPath = f"${env.dataDirectory}/$databaseDataset"
//    queryDatasetPath    = f"${env.dataDirectory}/$queryDataset"
//  } yield {
//    test(s"Example test, databio dataset: ${benchmark.description} on $dataSuite") {
//      implicit val sparkSession: SparkSession = spark
//      val testData = TestDataLoader.load(dataSuite, databaseDatasetPath, queryDatasetPath)
//      BenchmarkRunner.run(testData, benchmark, writer, Duration.Inf)
//    }
//  }
}
