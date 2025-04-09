package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.domain.test.TestDataRow
import org.apache.spark.sql.{Dataset, SparkSession}


object TestDataGenerator {

  def generateLinear(clustersCount: Int, rowsPerCluster: Long, rowLength: Int = 1)(implicit spark: SparkSession): Dataset[TestDataRow] = {
    (1 to clustersCount)
      .map(cluster => generateLinear(f"CH-$cluster", rowsPerCluster, rowLength))
      .reduce(_.unionByName(_))
  }

  def generateLinear(cluster: String, rowsToGenerate: Long, rowLength: Int)(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    spark.sparkContext.range(1L, rowsToGenerate + 1)
      .map(i => TestDataRow(i, i + rowLength, cluster, ""))
      .map(row => addValue(row))
      .toDS()
  }

  def addValue(testDataRow: TestDataRow): TestDataRow =
    testDataRow.copy(value = f"${testDataRow.key}-${testDataRow.from}-${testDataRow.to}")
}
