package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.test.data.generator.model
import me.kosik.interwalled.test.data.generator.model.{TestDataRow, TestResultRow}
import org.apache.spark.sql.{Dataset, SparkSession}

case class TestOneToOne(rowsCount: Long) extends TestCase {

  override def testCaseName: String = "one-to-one"

  override def generateLHS(implicit spark: SparkSession): Dataset[model.TestDataRow] =
    generateLinear(rowsCount)

  override def generateRHS(implicit spark: SparkSession): Dataset[model.TestDataRow] =
    generateLinear(rowsCount)

  override def generateResult(implicit spark: SparkSession): Dataset[model.TestResultRow] = {
    import spark.implicits._

    spark.sparkContext.range(1L, rowsCount + 1)
      .map(i => TestResultRow(i, i, i, i, "CH1"))
      .toDS()
  }
}
