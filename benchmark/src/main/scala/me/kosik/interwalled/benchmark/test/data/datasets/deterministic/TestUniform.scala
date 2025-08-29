package me.kosik.interwalled.benchmark.test.data.datasets.deterministic

import me.kosik.interwalled.benchmark.test.data.model.{IntervalLength, IntervalMargin, RawTestDataRow, TestDataFilter}
import me.kosik.interwalled.benchmark.test.data.datasets.TestCase
import org.apache.spark.sql.{Dataset, SparkSession}


class TestUniform(
  name: String,
  length: IntervalLength,
  margin: IntervalMargin,
  totalRowsCount: Long,
  additionalFilter: TestDataFilter = TestDataFilter.default
) extends TestCase with Serializable {

  override def testCaseName: String = name

  override def _generate()(implicit sparkSession: SparkSession): Dataset[RawTestDataRow] = {
    import sparkSession.implicits._

    sparkSession.sparkContext
      .range(0L, totalRowsCount)
      .map(i => RawTestDataRow(i * (length.value + margin.value), i * (length.value + margin.value) + length.value))
      .filter(_.from <= totalRowsCount)
      .filter(_.to   <= totalRowsCount)
      .filter(additionalFilter.fn)
      .toDS()
  }
}
