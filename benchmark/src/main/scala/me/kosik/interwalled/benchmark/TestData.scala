package me.kosik.interwalled.benchmark

import me.kosik.interwalled.domain.Interval
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestData(
  suite: String,
  clustersCount: Int,
  rowsPerCluster: Long,
  database: Dataset[Interval[String]],
  query: Dataset[Interval[String]]
) {
  override def toString: String = s"TestData($suite, $rowsPerCluster, $clustersCount)"

  def sparkSession: SparkSession =
    database.sparkSession
}


case class TestDataBuilder(pathPrefix: String, suite: String, clustersCount: Int, rowsPerCluster: Long) {
  def apply(spark: SparkSession): TestData = TestData(
    suite,
    clustersCount,
    rowsPerCluster,
    load(s"$pathPrefix/$suite/$rowsPerCluster/$clustersCount/database.parquet", spark),
    load(s"$pathPrefix/$suite/$rowsPerCluster/$clustersCount/query.parquet", spark)
  )

  override def toString: String = s"TestData($suite, $rowsPerCluster, $clustersCount)"

  private def load(path: String, spark: SparkSession): Dataset[Interval[String]] = {
    import spark.implicits._

    spark.read.parquet(path)
      .as[Interval[String]]
  }
}
