package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.{functions => F}


case class TestSparse(clustersCount: Int, rowsPerCluster: Long, rowsPerJoin: Int) extends TestCase {

  override def testCaseName: String = s"sparse-$rowsPerJoin"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    TestDataGenerator
      .generateLinear(clustersCount, rowsPerCluster / rowsPerJoin)
      .select(
        (F.col("from") * F.lit(rowsPerJoin)).as("from"),
        (F.col("to")   * F.lit(rowsPerCluster)).as("to"),
        F.col("key"),
        F.concat(
          F.col("key"),
          F.lit("-"),
          F.col("from") * F.lit(rowsPerJoin),
          F.lit("-"),
          F.col("from") * F.lit(rowsPerJoin)
        ).as("value")
      )
      .as[TestDataRow]
  }

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    TestDataGenerator.generateLinear(clustersCount, rowsPerCluster)

  override def generateResult(implicit spark: SparkSession): Dataset[TestResultRow] = {
    import spark.implicits._

    generateLHS
      .map(lhs => TestResultRow(
        lhs,
        lhs,
        lhs.key
      ))
  }
}
