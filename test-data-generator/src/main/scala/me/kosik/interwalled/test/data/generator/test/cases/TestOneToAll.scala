package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestOneToAll(rowsCount: Long) extends TestCase {

  override def testCaseName: String = "one-to-all"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    generateLinear(rowsCount)

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    List(TestDataRow(1L, rowsCount + 1, "CH1")).toDS().repartition(1)
  }

  override def generateResult(implicit spark: SparkSession): Dataset[TestResultRow] = {
    import spark.implicits._

    spark.sparkContext.range(1L, rowsCount + 1)
      .map(i => TestResultRow(i, i, 1L, rowsCount + 1, "CH1"))
      .toDS()
  }
}
