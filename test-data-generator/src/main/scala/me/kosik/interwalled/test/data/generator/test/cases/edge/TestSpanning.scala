package me.kosik.interwalled.test.data.generator.test.cases.edge

import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import me.kosik.interwalled.test.data.generator.test.cases.{TestCase, TestDataGenerator}
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestSpanning(clustersCount: Int, rowsPerCluster: Long, span: Int) extends TestCase {

  override def testCaseName: String = s"spanning-$span"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    TestDataGenerator
      .generateLinear(clustersCount, rowsPerCluster)
      .map { row => row.copy(
        from  = row.from - span,
        to    = row.to   + span
      )}
  }

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    TestDataGenerator.generateLinear(clustersCount, rowsPerCluster)

  override def generateResult(implicit spark: SparkSession): Option[Dataset[TestResultRow]] = Some {
    import spark.implicits._

    generateLHS(spark)
      .flatMap { lhsRow => (math.max(lhsRow.from, 1) to math.min(lhsRow.to, rowsPerCluster)).map { i =>
        val rhsRow = TestDataGenerator.addValue(TestDataRow(i, i, lhsRow.key, ""))
        TestResultRow(lhsRow, rhsRow, lhsRow.key)
      }}
  }
}
