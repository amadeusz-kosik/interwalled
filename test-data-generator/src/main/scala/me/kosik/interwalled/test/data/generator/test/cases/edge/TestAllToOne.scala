package me.kosik.interwalled.test.data.generator.test.cases.edge

import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import me.kosik.interwalled.test.data.generator.test.cases.{TestCase, TestDataGenerator}
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestAllToOne(clustersCount: Int, rowsPerCluster: Long) extends TestCase {

  override def testCaseName: String = "all-to-one"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    (1 to clustersCount)
      .map(cluster => TestDataRow(1L, rowsPerCluster + 1, f"CH-$cluster", f"CH-$cluster-1-${rowsPerCluster + 1}"))
      .toDS()
      .repartition(1)
  }

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    TestDataGenerator.generateLinear(clustersCount, rowsPerCluster)

  override def generateResult(implicit spark: SparkSession): Option[Dataset[TestResultRow]] = Some {
    import spark.implicits._

    generateRHS
      .map(rhs => TestResultRow(
        TestDataRow(1L, rowsPerCluster + 1, rhs.key, f"${rhs.key}-1-${rowsPerCluster + 1}"),
        rhs,
        rhs.key
      ))
  }
}
