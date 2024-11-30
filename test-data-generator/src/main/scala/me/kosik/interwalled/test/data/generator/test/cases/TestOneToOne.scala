package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestOneToOne(rowsCount: Long) extends TestCase {

  override def testCaseName: String = "one-to-one"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    generateLinear(rowsCount)

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    generateLinear(rowsCount)

  override def generateResult(implicit spark: SparkSession): Dataset[TestResultRow] = {
    import spark.implicits._

    spark.sparkContext.range(1L, rowsCount + 1)
      .map(i => TestResultRow(i, i, i, i, "CH1"))
      .toDS()
  }
}
