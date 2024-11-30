package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import org.apache.spark.sql.{Dataset, SparkSession}


trait TestCase {
  def testCaseName: String

  def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow]

  def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow]

  def generateResult(implicit spark: SparkSession): Dataset[TestResultRow]

  final val V_KEY   : String = "CH1"
  final val V_VALUE : String = "CONST_VALUE"

  final protected def generateLinear(rowsCount: Long)(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    spark.sparkContext.range(1L, rowsCount + 1)
      .map(i => TestDataRow(i, i, V_KEY, V_VALUE))
      .toDS()
  }
}
