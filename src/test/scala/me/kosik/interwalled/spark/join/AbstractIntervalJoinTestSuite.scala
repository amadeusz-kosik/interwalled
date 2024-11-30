package me.kosik.interwalled.spark.join

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import me.kosik.interwalled.domain.{Interval, IntervalsPair}
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite


abstract class AbstractIntervalJoinTestSuite extends AnyFunSuite with DataFrameSuiteBase with BeforeAndAfterEach {

  def inputSizes: Array[Long] = Array(100L, 1_000L)

  def inputSuites: Array[String] = Array("one-to-all", "one-to-many", "one-to-one")

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

    assertDataFrameDataEquals(expected = prepareResult(expected), result = prepareResult(actual))
  }

  // ---------------------------------------------------------------------------------------------------------------- //

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc.setLogLevel(Level.WARN.toString)
  }

  inputSizes foreach { inputSize => inputSuites.foreach { inputSuite =>
    test(s"$inputSize rows, $inputSuite") {
      import spark.implicits._

      def loadInput(datasetName: String): Dataset[Interval[String]] =
        spark.read.parquet(s"data/$inputSuite/$inputSize/$datasetName.parquet")
          .as[Interval[String]]

      def loadResult(datasetName: String): Dataset[IntervalsPair[String]] =
        spark.read.parquet(s"data/$inputSuite/$inputSize/$datasetName.parquet")
          .as[IntervalsPair[String]]

      val lhs = loadInput("in-lhs").as[Interval[String]]
      val rhs = loadInput("in-rhs").as[Interval[String]]

      val expected = loadResult("out-result")
      val actual   = intervalJoin.join(lhs, rhs)

      assertDataEqual(expected = expected, actual = actual)
    }
  }}
}
