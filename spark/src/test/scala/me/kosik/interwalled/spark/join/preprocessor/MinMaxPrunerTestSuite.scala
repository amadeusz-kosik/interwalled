package me.kosik.interwalled.spark.join.preprocessor

import com.holdenkarau.spark.testing.DatasetSuiteBase
import me.kosik.interwalled.ailist.BucketedInterval
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.preprocessor.MinMaxPruner.MinMaxPrunerConfig
import org.scalactic.source
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

class MinMaxPrunerTestSuite extends AnyFunSuite with DatasetSuiteBase {

  private def minMaxPrunerTest(testName: String, testTags: Tag*)(given: => PreparedInput)(expected: => PreparedInput)(implicit pos: source.Position): Unit = {
    test(testName = testName, testTags = testTags: _*) {
      val minMaxPruner = new MinMaxPruner(MinMaxPrunerConfig(true))
      val actual = minMaxPruner.processInput(given)
      assertDatasetEquals(expected.lhsData, actual.lhsData)
      assertDatasetEquals(expected.rhsData, actual.rhsData)
    }
  }

  minMaxPrunerTest("MinMaxPruner should not filter out rows in case of fully overlapping input") {
    import spark.implicits._
    PreparedInput(
      lhsData = Seq(
        BucketedInterval("", "K", 0, 10, "test-data"),
        BucketedInterval("", "K", 5, 15, "test-data"),
        BucketedInterval("", "K", 10, 20, "test-data"),
        BucketedInterval("", "K", 15, 25, "test-data"),
        BucketedInterval("", "K", 20, 30, "test-data")
      ).toDS(),
      rhsData = Seq(
        BucketedInterval("", "K",  9, 19, "test-data"),
        BucketedInterval("", "K", 13, 23, "test-data"),
        BucketedInterval("", "K", 17, 27, "test-data"),
        BucketedInterval("", "K", 11, 21, "test-data"),
        BucketedInterval("", "K", 15, 25, "test-data")
      ).toDS()
    )
  } {
    import spark.implicits._
    PreparedInput(
      lhsData = Seq(
        BucketedInterval("", "K", 0, 10, "test-data"),
        BucketedInterval("", "K", 5, 15, "test-data"),
        BucketedInterval("", "K", 10, 20, "test-data"),
        BucketedInterval("", "K", 15, 25, "test-data"),
        BucketedInterval("", "K", 20, 30, "test-data")
      ).toDS(),
      rhsData = Seq(
        BucketedInterval("", "K",  9, 19, "test-data"),
        BucketedInterval("", "K", 13, 23, "test-data"),
        BucketedInterval("", "K", 17, 27, "test-data"),
        BucketedInterval("", "K", 11, 21, "test-data"),
        BucketedInterval("", "K", 15, 25, "test-data")
      ).toDS()
    )
  }

  minMaxPrunerTest("MinMaxPruner should filter out non-overlapping parts in overlapping input") {
    import spark.implicits._
    PreparedInput(
      lhsData = Seq(
        BucketedInterval("", "K", 0, 10, "test-data"),
        BucketedInterval("", "K", 5, 15, "test-data"),
        BucketedInterval("", "K", 10, 20, "test-data"),
        BucketedInterval("", "K", 15, 25, "test-data"),
        BucketedInterval("", "K", 20, 30, "test-data")
      ).toDS(),
      rhsData = Seq(
        BucketedInterval("", "K", 19, 29, "test-data"),
        BucketedInterval("", "K", 23, 33, "test-data"),
        BucketedInterval("", "K", 27, 37, "test-data"),
        BucketedInterval("", "K", 31, 41, "test-data"),
        BucketedInterval("", "K", 35, 45, "test-data")
      ).toDS()
    )
  } {
    import spark.implicits._
    PreparedInput(
      lhsData = Seq(
        BucketedInterval("", "K", 10, 20, "test-data"),
        BucketedInterval("", "K", 15, 25, "test-data"),
        BucketedInterval("", "K", 20, 30, "test-data")
      ).toDS(),
      rhsData = Seq(
        BucketedInterval("", "K", 19, 29, "test-data"),
        BucketedInterval("", "K", 23, 33, "test-data"),
        BucketedInterval("", "K", 27, 37, "test-data")
      ).toDS()
    )
  }

  minMaxPrunerTest("MinMaxPruner should filter out non-overlapping parts in non-overlapping input") {
    import spark.implicits._
    PreparedInput(
      lhsData = Seq(
        BucketedInterval("", "K", 0, 10, "test-data"),
        BucketedInterval("", "K", 5, 15, "test-data"),
        BucketedInterval("", "K", 10, 20, "test-data"),
        BucketedInterval("", "K", 15, 25, "test-data"),
        BucketedInterval("", "K", 20, 30, "test-data")
      ).toDS(),
      rhsData = Seq(
        BucketedInterval("", "K", 49, 59, "test-data"),
        BucketedInterval("", "K", 53, 63, "test-data"),
        BucketedInterval("", "K", 57, 67, "test-data"),
        BucketedInterval("", "K", 51, 61, "test-data"),
        BucketedInterval("", "K", 55, 65, "test-data")
      ).toDS()
    )
  } {
    import spark.implicits._
    PreparedInput(
      lhsData = Seq.empty[BucketedInterval].toDS(),
      rhsData = Seq.empty[BucketedInterval].toDS()
    )
  }
}
