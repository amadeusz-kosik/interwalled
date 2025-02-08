package me.kosik.interwalled.benchmark

import me.kosik.interwalled.domain.Interval
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestData(
  description: String,
  database: Dataset[Interval[String]],
  query: Dataset[Interval[String]]
)


object TestData {
  def fromPath(pathPrefix: String, suite: String, size: Long, sparkSession: SparkSession): TestData = TestData(
    s"$suite: $size",
    load(s"$pathPrefix/$suite/$size/database.parquet", sparkSession),
    load(s"$pathPrefix/$suite/$size/query.parquet", sparkSession)
  )

  private def load(path: String, spark: SparkSession): Dataset[Interval[String]] = {
    import spark.implicits._

    spark.read.parquet(path)
      .as[Interval[String]]
  }
}