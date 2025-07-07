package me.kosik.interwalled.spark.join

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import me.kosik.interwalled.domain.benchmark.ActiveBenchmarks.TestDataSizes
import me.kosik.interwalled.domain.{Interval, IntervalsPair}
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.funsuite.AnyFunSuite


abstract class AbstractIntervalJoinTestSuite extends AnyFunSuite with DataFrameSuiteBase {

  val inputSizes: Array[(Int, Long)] = TestDataSizes.baseline.take(1)

  def inputSuites: Array[String] =
    Array("all-to-one", "one-to-all", "one-to-one", "spanning-4", "spanning-16")

  def intervalJoin: IntervalJoin

  // ---------------------------------------------------------------------------------------------------------------- //

  def assertDataEqual(expected: Dataset[IntervalsPair[String]], actual: Dataset[IntervalsPair[String]]): Unit = {
    import expected.sparkSession.implicits._

    def prepareResult(data: Dataset[IntervalsPair[String]]): DataFrame = data
      .toDF()
      .select(
        $"lhs.from" .as("lhs_from"),
        $"lhs.to"   .as("lhs_to"),
        $"lhs.key"  .as("lhs_key"),
        $"lhs.value".as("lhs_value"),
        $"rhs.from" .as("rhs_from"),
        $"rhs.to"   .as("rhs_to"),
        $"rhs.key"  .as("rhs_key"),
        $"rhs.value".as("rhs_value"),
      )

    assertDataFrameNoOrderEquals(expected = prepareResult(expected), result = prepareResult(actual))
  }

  // ---------------------------------------------------------------------------------------------------------------- //

  inputSizes foreach { case (clustersCount, rowsPerCluster) => inputSuites.foreach { inputSuite =>

    test(s"$rowsPerCluster rows, $inputSuite, $clustersCount clusters") {
      import spark.implicits._

      def loadInput(datasetName: String): Dataset[Interval[String]] =
        spark.read.parquet(s"data/$inputSuite/$rowsPerCluster/$clustersCount/$datasetName.parquet")
          .as[Interval[String]]

      def loadResult(datasetName: String): Dataset[IntervalsPair[String]] =
        spark.read.parquet(s"data/$inputSuite/$rowsPerCluster/$clustersCount/$datasetName.parquet")
          .as[IntervalsPair[String]]

      val lhs = loadInput("database").as[Interval[String]]
      val rhs = loadInput("query").as[Interval[String]]

      val expected = loadResult("result")
      val actual = intervalJoin.join(Input(lhs, rhs)).data

      assertDataEqual(expected = expected, actual = actual)
    }

  }}
}
