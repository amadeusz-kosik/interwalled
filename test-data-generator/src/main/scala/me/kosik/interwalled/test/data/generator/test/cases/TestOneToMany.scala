package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestOneToMany(lhsRowsCount: Long, lhsPerRhs: Int) extends TestCase {

  override def testCaseName: String = s"one-to-many-$lhsPerRhs"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    generateLinear(lhsRowsCount)

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    spark.sparkContext.range(1L, lhsRowsCount, step = lhsPerRhs)
      .map(i => TestDataRow(i, i + lhsPerRhs - 1, V_KEY, V_VALUE))
      .toDS()
  }

  override def generateResult(implicit spark: SparkSession): Dataset[TestResultRow] = {
    import spark.implicits._

    spark.sparkContext.range(1L, lhsRowsCount, step = lhsPerRhs)
      .flatMap { iRHS =>
        (0 until lhsPerRhs).map { iLHS => TestResultRow(
          TestDataRow(iLHS + iRHS, iLHS + iRHS, V_KEY, V_VALUE),
          TestDataRow(iRHS, iRHS + lhsPerRhs - 1, V_KEY, V_VALUE),
          V_KEY
        )}
      }
      .toDS()
  }
}
