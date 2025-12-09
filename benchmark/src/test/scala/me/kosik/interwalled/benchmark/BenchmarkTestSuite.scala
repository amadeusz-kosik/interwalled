package me.kosik.interwalled.benchmark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import me.kosik.interwalled.benchmark.app.ApplicationEnv
import me.kosik.interwalled.benchmark.join.JoinStrategies
import me.kosik.interwalled.benchmark.test.suite.{TestDataSuite, TestDataSuiteReader}
import me.kosik.interwalled.benchmark.utils.csv.{BenchmarkCSVFormatter, CSVWriter}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.PrintWriter


class BenchmarkTestSuite extends AnyFunSuite with DataFrameSuiteBase {
  private lazy val env = ApplicationEnv.buildTest("benchmark-test-suite")
  private lazy val writer = CSVWriter.forWritter(BenchmarkCSVFormatter)(new PrintWriter(System.out))

  // ---------------------------------------------------------------------------------------------------------------- //

  private lazy val joinStrategies = JoinStrategies
    .values

  private lazy val syntheticDatasets = Array(
    TestDataSuite("odd-to-even", "test-data/single-point-even.parquet", "single-point-odd.parquet", None)
  )

//  for {
//    (joinStrategyName, joinStrategy)  <- joinStrategies.take(3)
//    testDataSuite                     <- syntheticDatasets
//  } yield {
//    test(s"Example test, edge case: ${joinStrategyName} on $testDataSuite") {
//      implicit val sparkSession: SparkSession = spark
//      val testData = TestDataSuiteReader.readQuery(testDataSuite, env)
////      BenchmarkRunner.run(testData, benchmark, writer, Duration.Inf)
//    }
//  }

}
