package me.kosik.interwalled.test.data.generator.test.cases.edge

import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import me.kosik.interwalled.test.data.generator.test.cases.{TestCase, TestDataGenerator}
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestOneToOne(clustersCount: Int, rowsPerCluster: Long) extends TestCase {

  override def testCaseName: String = "one-to-one"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    TestDataGenerator.generateLinear(clustersCount, rowsPerCluster)

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    TestDataGenerator.generateLinear(clustersCount, rowsPerCluster)

  override def generateResult(implicit spark: SparkSession): Option[Dataset[TestResultRow]] = Some {
    import spark.implicits._

    generateLHS
      .map(lhs => TestResultRow(
        lhs,
        lhs,
        lhs.key
      ))
  }
}
