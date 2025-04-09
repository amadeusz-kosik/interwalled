package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestAllToAll(clustersCount: Int, rowsPerCluster: Long) extends TestCase {

  override def testCaseName: String = "all-to-all"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    generateHS(spark)

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    generateHS(spark)

  override def generateResult(implicit spark: SparkSession): Option[Dataset[TestResultRow]] = None

  private def generateHS(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._
    val rowsPerClusterSqrt = math.sqrt(rowsPerCluster).toLong

    TestDataGenerator
      .generateLinear(clustersCount, rowsPerClusterSqrt)
      .map { testDataRow => testDataRow.copy(
        from = 1L,
        to   = rowsPerClusterSqrt,
        value = s"${testDataRow.key}-${testDataRow.from}"
      )}
  }
}
