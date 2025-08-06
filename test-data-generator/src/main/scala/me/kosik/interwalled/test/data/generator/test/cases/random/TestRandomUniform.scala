package me.kosik.interwalled.test.data.generator.test.cases.random

import me.kosik.interwalled.domain.IntervalColumns
import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import me.kosik.interwalled.test.data.generator.test.cases.{TestCase, TestDataGenerator}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, SparkSession, functions => F}


case class TestRandomUniform(clustersCount: Int, linearLength: Int, rowsPerCluster: Long) extends TestCase {

  override def testCaseName: String = "one-to-all"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    TestDataGenerator.generateLinear(clustersCount, rowsPerCluster, linearLength)

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    TestDataGenerator
        .generateLinear(clustersCount, rowsPerCluster)
        .withColumn(IntervalColumns.TO, F.col(IntervalColumns.TO) + F.ceil(F.rand() * 10).cast(DataTypes.IntegerType))
        .as[TestDataRow]
  }

  override def generateResult(implicit spark: SparkSession): Option[Dataset[TestResultRow]] = None
}
