package me.kosik.interwalled.spark.join

import com.holdenkarau.spark.testing.DatasetSuiteBase
import me.kosik.interwalled.model.{SparkInterval, SparkIntervalsPair}
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import org.apache.spark.sql.{Dataset, functions => F}
import org.scalatest.funsuite.AnyFunSuite


abstract class AbstractIntervalJoinTestSuite extends AnyFunSuite with DatasetSuiteBase {

  def intervalJoin: IntervalJoin

  // ---------------------------------------------------------------------------------------------------------------- //

  def assertDataEquals(expected: Dataset[SparkIntervalsPair], actual: Dataset[SparkIntervalsPair]): Unit = {
    def prepareResult(data: Dataset[SparkIntervalsPair]) = data
      .sort(F.col("key"), F.col("lhsFrom"), F.col("lhsTo"), F.col("rhsFrom"), F.col("rhsTo"))

    assertDatasetEquals(expected = prepareResult(expected), result = prepareResult(actual))
  }

  // ---------------------------------------------------------------------------------------------------------------- //

  test(s"Test one-to-one join") {
    import spark.implicits._

    val lhs = {
      spark.sparkContext
        .range(1L, 100000L + 1L, 10L)
        .map(i => SparkInterval("KEY", i, i + 9, f"KEY($i - ${i + 9})"))
        .toDS()
    }

    val rhs = {
      spark.sparkContext
        .range(1L, 100000L + 1L, 10L)
        .map(_ + 5L)
        .map(i => SparkInterval("KEY", i, i, f"KEY($i - $i})"))
        .toDS()
    }

    val expected = {
      spark.sparkContext
        .range(1L, 100000L + 1L, 10L)
        .map(i => SparkIntervalsPair(
          SparkInterval("KEY", i,     i + 9, f"KEY($i - ${i + 9})"),
          SparkInterval("KEY", i + 5, i + 5, f"KEY(${i + 5} - ${i + 5}})")
        ))
        .toDS()
    }

    val actual = intervalJoin.join(Input(lhs, rhs)).data
    assertDataEquals(expected = expected, actual = actual)
  }
}
