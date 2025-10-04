package me.kosik.interwalled.spark.join

import com.holdenkarau.spark.testing.DatasetSuiteBase
import me.kosik.interwalled.domain.{Interval, IntervalsPair}
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import org.apache.spark.sql.{Dataset, functions => F}
import org.scalatest.funsuite.AnyFunSuite


abstract class AbstractIntervalJoinTestSuite extends AnyFunSuite with DatasetSuiteBase {

  def intervalJoin: IntervalJoin

  // ---------------------------------------------------------------------------------------------------------------- //

  def assertDataEquals(expected: Dataset[IntervalsPair], actual: Dataset[IntervalsPair]): Unit = {
    def prepareResult(data: Dataset[IntervalsPair]) = data
      .sort(F.col("key"), F.col("lhs.from"), F.col("lhs.to"), F.col("rhs.from"), F.col("rhs.to"))

    assertDatasetEquals(expected = prepareResult(expected), result = prepareResult(actual))
  }

  // ---------------------------------------------------------------------------------------------------------------- //

  test(s"Test one-to-one join") {
    import spark.implicits._

    val lhs = {
      spark.sparkContext
        .range(1L, 100000L + 1L, 10L)
        .map(i => Interval("KEY", i, i + 9, f"KEY($i - ${i + 9})"))
        .toDS()
    }

    val rhs = {
      spark.sparkContext
        .range(1L, 100000L + 1L, 10L)
        .map(_ + 5L)
        .map(i => Interval("KEY", i, i, f"KEY($i - $i})"))
        .toDS()
    }

    val expected = {
      spark.sparkContext
        .range(1L, 100000L + 1L, 10L)
        .map(i => IntervalsPair("KEY",
          Interval("KEY", i,     i + 9, f"KEY($i - ${i + 9})"),
          Interval("KEY", i + 5, i + 5, f"KEY(${i + 5} - ${i + 5}})")
        ))
        .toDS()
    }

    val actual = intervalJoin.join(Input(lhs, rhs)).data
    assertDataEquals(expected = expected, actual = actual)
  }
}
