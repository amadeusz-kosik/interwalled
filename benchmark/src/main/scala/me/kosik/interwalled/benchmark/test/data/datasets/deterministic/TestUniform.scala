package me.kosik.interwalled.benchmark.test.data.datasets.deterministic

import me.kosik.interwalled.benchmark.test.data.model.{IntervalLength, IntervalMargin, RawTestDataRow}
import me.kosik.interwalled.benchmark.test.data.datasets.TestCase
import me.kosik.interwalled.benchmark.test.data.datasets.deterministic.TestUniform.{TestDataFilter, TestDataMapping}
import org.apache.spark.sql.{Dataset, SparkSession}


class TestUniform(
  name: String,
  length: IntervalLength,
  margin: IntervalMargin,
  rowsLimitRight: Long,
  additionalMapping: TestDataMapping = TestDataMapping.default,
  additionalFilter: TestDataFilter = TestDataFilter.default
) extends TestCase with Serializable {

  override def testCaseName: String = name

  override def _generate()(implicit sparkSession: SparkSession): Dataset[RawTestDataRow] = {
    import sparkSession.implicits._

    sparkSession.sparkContext
      .range(0L, rowsLimitRight)
      .map(i => RawTestDataRow(i * (length.value + margin.value), i * (length.value + margin.value) + length.value))
      .map(additionalMapping.fn)
      .filter(_.from <= rowsLimitRight)
      .filter(_.to   <= rowsLimitRight)
      .filter(additionalFilter.fn)
      .toDS()
  }
}

object TestUniform {

  case class TestDataMapping(fn: RawTestDataRow => RawTestDataRow)

  object TestDataMapping {
    def default: TestDataMapping = TestDataMapping(identity[RawTestDataRow])
  }

  case class TestDataFilter(fn: RawTestDataRow => Boolean)

  object TestDataFilter {
    def default: TestDataFilter = TestDataFilter(_ => true)
  }
}