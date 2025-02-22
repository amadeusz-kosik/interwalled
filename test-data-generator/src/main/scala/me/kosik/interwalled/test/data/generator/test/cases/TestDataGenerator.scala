package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.domain.test.TestDataRow
import org.apache.spark.sql.{Dataset, SparkSession}


object TestDataGenerator {

  def generateLinear(clustersCount: Int, rowsPerCluster: Long)(implicit spark: SparkSession): Dataset[TestDataRow] = {
    (1 to clustersCount)
      .map(cluster => generateLinear(f"CH-$cluster", rowsPerCluster))
      .reduce(_.unionByName(_))
  }

  def generateLinear(cluster: String, rowsToGenerate: Long)(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    spark.sparkContext.range(1L, rowsToGenerate + 1)
      .map(i => TestDataRow(i, i, cluster, f"CH-$cluster-$i-$i"))
      .toDS()
  }
}
