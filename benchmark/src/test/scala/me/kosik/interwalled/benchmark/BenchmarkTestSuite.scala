package me.kosik.interwalled.benchmark

import com.holdenkarau.spark.testing.DatasetSuiteBase
import me.kosik.interwalled.benchmark.app.ApplicationEnv
import me.kosik.interwalled.benchmark.join.JoinStrategies
import me.kosik.interwalled.benchmark.test.suite.unit.UnitTestDataSuite
import me.kosik.interwalled.benchmark.utils.csv.{BenchmarkCSVFormatter, CSVWriter}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import org.scalatest.funsuite.AnyFunSuite

import java.io.PrintWriter


class BenchmarkTestSuite extends AnyFunSuite with DatasetSuiteBase {
  private lazy val env = ApplicationEnv.buildTest("benchmark-test-suite")
  private lazy val writer = CSVWriter.forWritter(BenchmarkCSVFormatter)(new PrintWriter(System.out))

  // ---------------------------------------------------------------------------------------------------------------- //

  private lazy val joinStrategies = JoinStrategies
    .values
    .filter { case (jsName, _) => ! jsName.contains("checkpoint") }


  for {
    (joinStrategyName, joinStrategy)  <- joinStrategies
    unitTestDataSuite                 <- UnitTestDataSuite.ALL_SUITES
  } yield {

    test(s"Example test, edge case: ${joinStrategyName} on ${unitTestDataSuite.name}") {
      val database = unitTestDataSuite.loadDatabase(env)
      val query    = unitTestDataSuite.loadQuery(env)

      val actual   = joinStrategy.join(Input(database, query), gatherStatistics = false).data
      val expected = unitTestDataSuite.loadResults(env)

      assertEmpty(expected.except(actual).take(10))
      assertEmpty(actual.except(expected).take(10))
    }
  }

}
