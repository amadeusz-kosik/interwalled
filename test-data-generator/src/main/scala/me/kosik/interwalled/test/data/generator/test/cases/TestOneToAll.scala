package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestOneToAll(clustersCount: Int, rowsPerCluster: Long) extends TestCase {

  override def testCaseName: String = "one-to-all"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    TestDataGenerator.generateLinear(clustersCount, rowsPerCluster)

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    (1 to clustersCount)
      .map(cluster => TestDataRow(1L, rowsPerCluster + 1, f"CH-$cluster", f"$cluster-1-${rowsPerCluster + 1}"))
      .toDS()
      .repartition(1)
  }

  override def generateResult(implicit spark: SparkSession): Option[Dataset[TestResultRow]] = Some {
    import spark.implicits._

    generateLHS
      .map(lhs => TestResultRow(
        lhs,
        TestDataRow(1L, rowsPerCluster + 1, f"CH-${lhs.key}", f"${lhs.key}-1-${rowsPerCluster + 1}"),
        lhs.key
      ))
  }
}
