package me.kosik.interwalled.benchmark

import me.kosik.interwalled.domain.Interval
import org.apache.spark.sql.{Dataset, SparkSession}

case class TestData(database: Dataset[Interval[String]], query: Dataset[Interval[String]])

object TestData {
  def fromPath(pathPrefix: String, sparkSession: SparkSession): TestData = TestData(
    load(s"$pathPrefix/database.parquet", sparkSession),
    load(s"$pathPrefix/query.parquet", sparkSession)
  )

  private def load(path: String, spark: SparkSession): Dataset[Interval[String]] = {
    import spark.implicits._

    spark.read.parquet(path)
      .as[Interval[String]]
  }
}