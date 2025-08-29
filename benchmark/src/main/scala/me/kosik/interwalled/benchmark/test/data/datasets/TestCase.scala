package me.kosik.interwalled.benchmark.test.data.datasets

import me.kosik.interwalled.benchmark.test.data.model.RawTestDataRow
import me.kosik.interwalled.domain.IntervalColumns
import me.kosik.interwalled.domain.test.TestDataRow
import org.apache.spark.sql.{Dataset, SparkSession, functions => F}


trait TestCase extends Serializable {
  def testCaseName: String

  def generate()(implicit sparkSession: SparkSession): Dataset[TestDataRow] =
    buildDataset(_generate())

  protected def _generate()(implicit sparkSession: SparkSession): Dataset[RawTestDataRow]

  private def buildDataset(data: Dataset[RawTestDataRow])(implicit sparkSession: SparkSession): Dataset[TestDataRow] = {
    import IntervalColumns._
    import sparkSession.implicits._

    data
      .withColumn(KEY,   F.lit(testCaseName))
      .withColumn(VALUE, F.concat_ws("-", F.col(KEY), F.col(FROM), F.col(TO)))
      .as[TestDataRow]
  }
}
